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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.internal.SQLConf

class ObjectAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    originalInputAttributes: Seq[Attribute],
    inputRows: Iterator[InternalRow],
    fallbackCountThreshold: Int)
  extends AggregationIterator(
    groupingExpressions,
    originalInputAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) with Logging {

  // This is the hash map used to store all groups and their corresponding aggregation buffers for
  // hash-based aggregation.
  private val hashMap = new ObjectAggregationMap(createNewAggregationBuffer())

  // The iterator created from the hash map. It is used to generate output rows when we are using
  // hash-based aggregation.
  private[this] var aggBufferMapIterator: ju.Iterator[ju.Map.Entry[UnsafeRow, MutableRow]] = _

  // The sorter used for sort-based aggregation when the hash map grows too large. Defaults to null
  // since we always try hash-based aggregation first.
  private[this] var externalSorter: UnsafeKVExternalSorter = _

  // Indicates if we are using sort-based aggregation.
  private[this] def sortBased: Boolean = externalSorter != null

  // The KVIterator containing input rows for the sort-based aggregation. It will be set in
  // `switchToSortBasedAggregation()` after we switch to sort-based aggregation.
  private[this] var sortedKVIterator: UnsafeKVExternalSorter#KVSorterIterator = _

  // The grouping key of the current group. Only used in sort-based aggregation.
  private[this] var currentGroupingKey: UnsafeRow = _

  // The grouping key of next group. Only used in sort-based aggregation.
  private[this] var nextGroupingKey: UnsafeRow = _

  // The first row of next group. Only used in sort-based aggregation.
  private[this] var firstRowInNextGroup: UnsafeRow = _

  // Indicates if we has new group of rows from the sorted input iterator. Only used in sort-based
  // aggregation.
  private[this] var sortedInputHasNewGroup: Boolean = false

  // The aggregation buffer used by the sort-based aggregation. Only used in sort-based aggregation.
  private[this] val sortBasedAggregationBuffer: MutableRow = createNewAggregationBuffer()

  // The function used to process rows in a group. Only used in sort-based aggregation.
  private[this] var sortBasedProcessRow: (MutableRow, InternalRow) => Unit = _

  /**
   * Start processing input rows.
   */
  processInputs()

  if (!sortBased) {
    aggBufferMapIterator = hashMap.iterator
  }

  override final def hasNext: Boolean = {
    (sortBased && sortedInputHasNewGroup) || (!sortBased && aggBufferMapIterator.hasNext)
  }

  override final def next(): UnsafeRow = {
    if (sortBased) {
      // Process the current group.
      processCurrentSortedGroup()
      val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
      // Re-initializes buffer values for the next group.
      initAggregationBuffer(sortBasedAggregationBuffer)
      outputRow
    } else {
      val entry = aggBufferMapIterator.next()
      generateOutput(entry.getKey, entry.getValue)
    }
  }

  /**
   * Generate an output row when there is no input and there is no grouping expression.
   */
  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    if (groupingExpressions.isEmpty) {
      val defaultAggregationBuffer = createNewAggregationBuffer()
      generateOutput(UnsafeRow.createFromByteArray(0, 0), defaultAggregationBuffer)
    } else {
      throw new IllegalStateException(
        "This method should not be called when groupingExpressions is not empty.")
    }
  }

  // Creates a new aggregation buffer and initializes buffer values. This function should only be
  // called under two cases:
  //
  //  - when creating aggregation buffer for a new group in the hash map, and
  //  - when creating the re-used buffer for sort-based aggregation
  private def createNewAggregationBuffer(): MutableRow = {
    val bufferFieldTypes = aggregateFunctions.flatMap(_.aggBufferAttributes.map(_.dataType))
    val buffer = new SpecificMutableRow(bufferFieldTypes)
    initAggregationBuffer(buffer)
    buffer
  }

  private def initAggregationBuffer(buffer: MutableRow): Unit = {
    // Initializes declarative aggregates' buffer values
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    // Initializes imperative aggregates' buffer values
    aggregateFunctions.collect { case f: ImperativeAggregate => f }.foreach(_.initialize(buffer))
  }

  // This function is used to read and process input rows. When processing input rows, it first uses
  // hash-based aggregation by putting groups and their buffers in `hashMap`. If `hashMap` grows too
  // large, it sorts the contents, spills them to disk, and creates a new map. At last, all sorted
  // spills are merged together for sort-based aggregation.
  private def processInputs(): Unit = {
    if (groupingExpressions.isEmpty) {
      // If there is no grouping expressions, we can just reuse the same buffer over and over again.
      val groupingKey = groupingProjection.apply(null)
      val buffer: MutableRow = hashMap.getAggregationBufferByKey(groupingKey)
      while (inputRows.hasNext) {
        val newInput = inputRows.next()
        processRow(buffer, newInput)
      }
    } else {
      while (inputRows.hasNext) {
        val newInput = inputRows.next()
        val groupingKey = groupingProjection.apply(newInput).copy()
        val buffer: MutableRow = hashMap.getAggregationBufferByKey(groupingKey)
        processRow(buffer, newInput)

        // The the hash map gets too large, makes a sorted spill and clear the map.
        if (hashMap.size >= fallbackCountThreshold) {
          logInfo(
            s"Aggregation hash map reaches threshold capacity ($fallbackCountThreshold entries), " +
              s"spilling and falling back to sort based aggregation. " +
              s"You may change the threshold by adjust option " +
              SQLConf.OBJECT_AGG_FALLBACK_COUNT_THRESHOLD.key
          )

          val sorter = hashMap.dumpToExternalSorter(groupingAttributes, aggregateFunctions)

          if (externalSorter == null) {
            // Actually this is where we officially switch to sort-based aggregation.
            // TODO Makes this more explicit...
            externalSorter = sorter
          } else {
            externalSorter.merge(sorter)
          }
        }
      }

      if (sortBased) {
        // We've fallen back to sort-based aggregation, spills the remaining data in the hash map
        // and merges the spill into the external sorter.
        if (hashMap.size > 0) {
          val newSorter = hashMap.dumpToExternalSorter(groupingAttributes, aggregateFunctions)
          externalSorter.merge(newSorter)
        }
        prepareForSortBasedAggregation()
      }
    }
  }

  private def prepareForSortBasedAggregation(): Unit = {
    logInfo("Falling back to sort based aggregation.")

    // Basically the value of the KVIterator returned by externalSorter
    // will be just aggregation buffer, so we rewrite the aggregateExpressions to reflect it.
    val newExpressions = aggregateExpressions.map {
      case agg @ AggregateExpression(_, Partial, _, _) =>
        agg.copy(mode = PartialMerge)
      case agg @ AggregateExpression(_, Complete, _, _) =>
        agg.copy(mode = Final)
      case other => other
    }
    val newFunctions = initializeAggregateFunctions(newExpressions, 0)
    val newInputAttributes = newFunctions.flatMap(_.inputAggBufferAttributes)
    sortBasedProcessRow = generateProcessRow(newExpressions, newFunctions, newInputAttributes)

    // Sets up iterator of sorted merged aggregation buffers of different groups from the sorter.
    sortedKVIterator = externalSorter.sortedIterator()

    // Pre-loads the first key-value pair from the sorted iterator to make hasNext idempotent.
    sortedInputHasNewGroup = sortedKVIterator.next()

    // Copy the first key and value (aggregation buffer).
    if (sortedInputHasNewGroup) {
      val key = sortedKVIterator.getKey
      val value = sortedKVIterator.getValue
      nextGroupingKey = key.copy()
      currentGroupingKey = key.copy()
      firstRowInNextGroup = value.copy()
    }
  }

  // Processes rows in the current group. It will stop when it find a new group.
  private def processCurrentSortedGroup(): Unit = {
    // First, we need to copy nextGroupingKey to currentGroupingKey.
    currentGroupingKey.copyFrom(nextGroupingKey)
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    sortBasedProcessRow(sortBasedAggregationBuffer, firstRowInNextGroup)

    // The search will stop when we see the next group or there is no
    // input row left in the iterator.
    // Pre-load the first key-value pair to make the condition of the while loop
    // has no action (we do not trigger loading a new key-value pair
    // when we evaluate the condition).
    var hasNext = sortedKVIterator.next()
    while (!findNextPartition && hasNext) {
      // Get the grouping key and value (aggregation buffer).
      val groupingKey = sortedKVIterator.getKey
      val inputAggregationBuffer = sortedKVIterator.getValue

      // Check if the current row belongs the current input row.
      if (currentGroupingKey.equals(groupingKey)) {
        sortBasedProcessRow(sortBasedAggregationBuffer, inputAggregationBuffer)

        hasNext = sortedKVIterator.next()
      } else {
        // We find a new group.
        findNextPartition = true
        nextGroupingKey.copyFrom(groupingKey)
        firstRowInNextGroup.copyFrom(inputAggregationBuffer)
      }
    }
    // We have not seen a new group. It means that there is no new row in the input
    // iterator. The current group is the last group of the sortedKVIterator.
    if (!findNextPartition) {
      sortedInputHasNewGroup = false
      sortedKVIterator.close()
    }
  }
}
