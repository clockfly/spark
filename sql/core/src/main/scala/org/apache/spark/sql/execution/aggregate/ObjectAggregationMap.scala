/*
 * Copyright © 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.sql.execution.aggregate

import java.{util => ju}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.expressions.{Attribute, MutableRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, TypedImperativeAggregate}
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter

/**
 * An aggregation map that supports using safe `MutableRow`s aggregation buffers, so that we can
 * support storing arbitrary Java objects as aggregate function states in the aggregation buffers.
 * This class is only used together with [[ObjectHashAggregateExec]].
 */
class ObjectAggregationMap(makeEmptyAggregationBuffer: => MutableRow) {
  private[this] val hashMap = new ju.LinkedHashMap[UnsafeRow, MutableRow]

  def getAggregationBufferByKey(groupingKey: UnsafeRow): MutableRow = {
    var aggBuffer = hashMap.get(groupingKey)
    if (aggBuffer == null) {
      aggBuffer = makeEmptyAggregationBuffer
      hashMap.put(groupingKey, aggBuffer)
    }

    aggBuffer
  }

  def size: Int = hashMap.size()

  def iterator: Iterator[AggregationBufferEntry] = {
    val iter = hashMap.entrySet().iterator()
    new Iterator[AggregationBufferEntry] {
      val result = new AggregationBufferEntry(null, null)
      override def hasNext: Boolean = {
        iter.hasNext
      }
      override def next(): AggregationBufferEntry = {
        val entry = iter.next()
        result.groupingKey = entry.getKey
        result.aggregationBuffer = entry.getValue
        result
      }
    }
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
        case agg: TypedImperativeAggregate[_] =>
          agg.serializeAggregateBufferInPlace(entry.aggregationBuffer)
        case _ =>
      }

      sorter.insertKV(
        entry.groupingKey,
        unsafeAggBufferProjection(entry.aggregationBuffer)
      )
    }

    hashMap.clear()
    sorter
  }

  def clear(): Unit = {
    hashMap.clear()
  }
}

// Stores the grouping key and aggregation buffer
class AggregationBufferEntry(var groupingKey: UnsafeRow, var aggregationBuffer: MutableRow)
