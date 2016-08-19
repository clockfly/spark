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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.PercentileApprox.{PercentileBuffer, PercentileUDT}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, QuantileSummaries, TypeUtils}
import org.apache.spark.sql.catalyst.util.QuantileSummaries.defaultCompressThreshold
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = """_FUNC_(col, p [, B]) - Returns an approximate pth percentile of a numeric column in the
     group. The B parameter (default: 1000) controls approximation accuracy at the cost of
     memory. Higher values yield better approximations.
    _FUNC_(col, array(p1 [, p2]...) [, B]) - Same as above, but accepts and returns an array of
     percentile values instead of a single one.
          """)
case class PercentileApprox(
    child: Expression,
    percentiles: Array[Double],
    B: Int,
    dataType: DataType,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends TypedImperativeAggregate[PercentileBuffer] {

  private val relativeError = Math.max(1.0d / B, 0.001)

  // "_FUNC_(col, p, B) / _FUNC_(col, array(p1, ...), B)"
  def this(child: Expression, percentiles: Expression, B: Expression) = {
    this(
      child = child,
      PercentileApprox.validatePercentiles(percentiles),
      PercentileApprox.validateB(B),
      PercentileApprox.getResultDataType(percentiles))
  }

  // "_FUNC_(col, p) / _FUNC_(col, array(p1, ...))"
  def this(child: Expression, percentile: Expression) = {
    this(child, percentile, Literal(1000))
  }

  override val aggregationBufferType: UserDefinedType[PercentileBuffer] = PercentileUDT

  override def createAggregationBuffer(): PercentileBuffer = new PercentileBuffer(relativeError)

  override def update(buffer: PercentileBuffer, input: InternalRow): Unit = {
    buffer.update(child.eval(input).asInstanceOf[Double])
  }

  override def merge(buffer: PercentileBuffer, input: PercentileBuffer): Unit = {
    buffer.merge(input)
  }

  override def eval(buffer: PercentileBuffer): Any = {
    val result = buffer.query(percentiles)
    dataType match {
      case _ if result.isEmpty => null
      case _: DoubleType => result(0)
      case _: ArrayType => new GenericArrayData(result)
    }
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): PercentileApprox =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): PercentileApprox =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = Seq(child)

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)

  private def childSql: String = dataType match {
    case _: DoubleType => s"${child.sql}, ${percentiles(0)}, $B"
    case _: ArrayType => s"${child.sql}, array(${percentiles.mkString(", ")}), $B"
  }

  override def prettyName: String = "percentile_approx"

  override def sql: String = s"$prettyName($childSql)"

  override def sql(isDistinct: Boolean): String = {
    if (isDistinct) s"$prettyName(DISTINCT $childSql)" else s"$prettyName($childSql)"
  }
}

object PercentileApprox {

  /** Validates the B expression and extracts its value. */
  private def validateB(B: Expression): Int = B match {
    case Literal(i: Int, IntegerType) if i > 0 => i
    case _ => throw new AnalysisException("The B argument must be a positive integer literal")
  }

  /** Validates the percentile(s) expression and extracts the percentile(s). */
  private def validatePercentiles(percentileExpression: Expression): Array[Double] = {
    val percentiles = (percentileExpression.dataType, percentileExpression.eval()) match {
      case (num: NumericType, v) => Array(TypeUtils.getNumeric(num).toDouble(v))
      case (ArrayType(elementType: NumericType, false), v: ArrayData) if v.numElements() > 0 =>
        v.array.map(TypeUtils.getNumeric(elementType).toDouble)
      case _ =>
        throw new AnalysisException("The percentile(s) must be a double or double array literal.")
    }

    percentiles.foreach { percentile =>
      if (percentile < 0D || percentile > 1D) {
        throw new AnalysisException("the percentile(s) must be >= 0 and <= 1.0")
      }
    }
    percentiles
  }

  def getResultDataType(percentileExpression: Expression): DataType = {
    percentileExpression.dataType match {
      case num: NumericType => DoubleType
      case _ => ArrayType(DoubleType, containsNull = false)
    }
  }

  // serializer for QuantileSummaries
  private val fieldSerializer = ExpressionEncoder[QuantileSummaries].resolveAndBind()

  class PercentileBuffer(private var summary: QuantileSummaries) {

    def this(relativeError: Double) = {
      this(new QuantileSummaries(defaultCompressThreshold, relativeError))
    }

    def merge(input: PercentileBuffer): Unit = summary = summary.merge(input.summary)

    def update(input: Double): Unit = summary = summary.insert(input)

    def query(percentiles: Array[Double]): Array[Double] = {
      if (summary.count == 0 || percentiles.length == 0) {
        Array.empty[Double]
      } else {
        val result = new Array[Double](percentiles.length)
        var i = 0
        while (i < percentiles.length) {
          result(i) = summary.query(percentiles(i))
          i += 1
        }
        result
      }
    }

    def toBytes: Array[Byte] = {
      summary = summary.compress()
      fieldSerializer.toRow(summary).asInstanceOf[UnsafeRow].getBytes
    }
  }

  private object PercentileUDT extends UserDefinedType[PercentileBuffer] {
    override def sqlType: DataType = BinaryType

    override def userClass: Class[PercentileBuffer] = classOf[PercentileBuffer]

    override def serialize(obj: PercentileBuffer): Any = obj.toBytes

    override def deserialize(datum: Any): PercentileBuffer = {
      val bytes = datum.asInstanceOf[Array[Byte]]
      val row = new UnsafeRow(1)
      row.pointTo(bytes, bytes.length)
      new PercentileBuffer(fieldSerializer.fromRow(row))
    }
  }
}
