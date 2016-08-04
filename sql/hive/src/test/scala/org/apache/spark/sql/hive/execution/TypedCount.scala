/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, ObjectAggregateFunction}
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(expr) - A testing aggregate function resembles COUNT " +
          "but implements ObjectAggregateFunction.")
case class TypedCount(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends ImperativeAggregate with ObjectAggregateFunction {

  def this(child: Expression) = this(child, 0, 0)

  override def children: Seq[Expression] = child :: Nil

  override def dataType: DataType = LongType

  override def nullable: Boolean = false

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def inputTypes: Seq[AbstractDataType] = AnyDataType :: Nil

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    AttributeReference("state", BinaryType)() :: Nil

  override lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  override val supportsPartial: Boolean = true

  override def initialize(buffer: MutableRow): Unit = {
    buffer.update(mutableAggBufferOffset, TypedCount.State(0L))
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    if (child.eval(input) != null) {
      getState(buffer).count += 1
    }
  }

  override def merge(buffer: MutableRow, input: InternalRow): Unit = {
    getState(buffer).count += deserializeState(input, inputAggBufferOffset).count
  }

  override def eval(input: InternalRow): Any = {
    deserializeState(input, mutableAggBufferOffset).count
  }

  override def serializeAggregateBuffer(buffer: MutableRow): Unit = {
    val byteStream = new ByteArrayOutputStream()
    val dataStream = new DataOutputStream(byteStream)
    dataStream.writeLong(getState(buffer).count)
    buffer.update(mutableAggBufferOffset, byteStream.toByteArray)
  }

  private def getState(aggBuffer: InternalRow): TypedCount.State =
    aggBuffer
      .get(mutableAggBufferOffset, ObjectType(classOf[TypedCount.State]))
      .asInstanceOf[TypedCount.State]

  private def deserializeState(buffer: InternalRow, offset: Int): TypedCount.State = {
    val byteStream = new ByteArrayInputStream(buffer.getBinary(offset))
    val dataStream = new DataInputStream(byteStream)
    TypedCount.State(dataStream.readLong())
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override val prettyName: String = "typed_count"
}

object TypedCount {
  case class State(var count: Long)
}
