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

package org.apache.spark.sql.execution.aggregate

import java.{util => ju}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, MutableRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, ObjectAggregateFunction}
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter

/**
 * An aggregation map that supports using safe `InternalRow`s (i.e. `GenericInternalRow` and
 * `SpecificMutableRow` as grouping keys and aggregation buffers, so that we can support storing
 * arbitrary Java objects as aggregate function states in the aggregation buffers. This class is
 * only used together with [[ObjectHashAggregateExec]].
 */
class ObjectAggregationMap(makeEmptyAggregationBuffer: => MutableRow) {
  def getAggregationBufferByKey(groupingKey: InternalRow): MutableRow = {
    var aggBuffer = hashMap.get(groupingKey)
    if (aggBuffer == null) {
      aggBuffer = makeEmptyAggregationBuffer
      hashMap.put(groupingKey, aggBuffer)
    }

    aggBuffer
  }

  def size: Int = hashMap.size()

  def iterator: ju.Iterator[ju.Map.Entry[InternalRow, MutableRow]] = {
    hashMap.entrySet().iterator()
  }

  /**
   * Dumps all entries into a newly created external sorter, clears the hash map, and returns the
   * external sorter.
   */
  def dumpToExternalSorter(
      groupingAttributes: Seq[Attribute],
      aggregateFunctions: Seq[AggregateFunction]): UnsafeKVExternalSorter = {
    val aggBufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val sorter = new UnsafeKVExternalSorter(
      StructType.fromAttributes(groupingAttributes),
      StructType.fromAttributes(aggBufferAttributes),
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      TaskContext.get().taskMemoryManager().pageSizeBytes,
      SparkEnv.get.conf.getLong(
        "spark.shuffle.spill.numElementsForceSpillThreshold",
        UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD),
      null
    )

    val mapIterator = iterator
    val unsafeAggBufferProjection =
      UnsafeProjection.create(aggBufferAttributes.map(_.dataType).toArray)

    while (mapIterator.hasNext) {
      val entry = mapIterator.next()
      aggregateFunctions.foreach {
        case agg: ObjectAggregateFunction => agg.serializeAggregateBuffer(entry.getValue)
        case _ =>
      }

      sorter.insertKV(
        entry.getKey.asInstanceOf[UnsafeRow],
        unsafeAggBufferProjection(entry.getValue)
      )
    }

    hashMap.clear()

    sorter
  }

  private[this] val hashMap = new ju.LinkedHashMap[InternalRow, MutableRow]
}
