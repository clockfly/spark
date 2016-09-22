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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.{BaseOrdering, GenerateOrdering}
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter

class ObjectAggregationIterator(
    outputAttributes: Seq[Attribute],
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

  // Indicates whether we have fall back to sort-based aggregation.
  private[this] var sortBased: Boolean = false

  private[this] var aggBufferIterator: Iterator[AggregationBufferEntry] = null

  private val mergeAggregationBuffer: (MutableRow, InternalRow) => Unit = {
    // Hack the aggregation mode to call AggregateFunction.merge to merge two aggregation buffers
    val newExpressions = aggregateExpressions.map {
      case agg@AggregateExpression(_, Partial, _, _) =>
        agg.copy(mode = PartialMerge)
      case agg@AggregateExpression(_, Complete, _, _) =>
        agg.copy(mode = Final)
      case other => other
    }
    val newFunctions = initializeAggregateFunctions(newExpressions, 0)
    val newInputAttributes = newFunctions.flatMap(_.inputAggBufferAttributes)
    generateProcessRow(newExpressions, newFunctions, newInputAttributes)
  }

  private[this] val safeProjection: Projection =
    FromUnsafeProjection(outputAttributes.map(_.dataType))

  private val inputUnsafeProjection =
    UnsafeProjection.create(StructType.fromAttributes(originalInputAttributes))

  /**
   * Start processing input rows.
   */
  processInputs()

  override final def hasNext: Boolean = {
    aggBufferIterator.hasNext
  }

  override final def next(): UnsafeRow = {
    val entry = aggBufferIterator.next()
    generateOutput(entry.groupingKey, entry.aggregationBuffer)
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
    // In-memory map to store aggregation buffer for hash-based aggregation.
    val hashMap = new ObjectAggregationMap(createNewAggregationBuffer())

    // If in-memory map is unable to stores all aggregation buffer, fallback to sort-based
    // aggregation backed for sorted physical storage.
    var sortBasedAggregationStore: SortBasedAggregationStore = null

    if (groupingExpressions.isEmpty) {
      // If there is no grouping expressions, we can just reuse the same buffer over and over again.
      val groupingKey = groupingProjection.apply(null)
      val buffer: MutableRow = hashMap.getAggregationBufferByKey(groupingKey)
      while (inputRows.hasNext) {
        val newInput = safeProjection(inputRows.next())
        processRow(buffer, newInput)
      }
    } else {
      while (inputRows.hasNext) {
        if (!sortBased) {
          val newInput = safeProjection(inputRows.next())
          val groupingKey = groupingProjection.apply(newInput).copy()
          val buffer: MutableRow = hashMap.getAggregationBufferByKey(groupingKey)
          processRow(buffer, newInput)

          // The the hash map gets too large, makes a sorted spill and clear the map.
          if (hashMap.size >= fallbackCountThreshold) {
            logInfo(
              s"Aggregation hash map reaches threshold " +
                s"capacity ($fallbackCountThreshold entries), spilling and falling back to sort" +
                s" based aggregation. You may change the threshold by adjust option " +
                SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key
            )

            // Fallbacks to sort-based aggregation
            sortBased = true

            val aggBufferSorterDumpedFromHashMap = hashMap
              .dumpToExternalSorter(groupingAttributes, aggregateFunctions)
            sortBasedAggregationStore = new SortBasedAggregationStore(
              aggBufferSorterDumpedFromHashMap,
              StructType.fromAttributes(originalInputAttributes),
              StructType.fromAttributes(groupingAttributes),
              processRow,
              mergeAggregationBuffer,
              createNewAggregationBuffer())
          }
        } else {
          val unsafeInputRow = inputRows.next() match {
            case unsafeRow: UnsafeRow => unsafeRow
            case row => inputUnsafeProjection(row)
          }
          val groupingKey = groupingProjection.apply(unsafeInputRow)
          sortBasedAggregationStore.addInput(groupingKey, unsafeInputRow)
        }
      }
    }

    if (sortBased) {
      sortBasedAggregationStore.destructiveIterator()
    } else {
      aggBufferIterator = hashMap.iterator
    }
  }
}

/**
 * Aggregation store used to do sort-based aggregation.
 *
 * @param initialAggBufferSorter initial external sorter which stores sorted aggregation buffers.
 *                               The aggregation buffers in this sorter is merged into
 *                               SortBasedAggregationStore.
 * @param inputSchema  The schema of input row
 * @param groupingSchema The schema of grouping key
 * @param updateInputRow  Function to update the aggregation buffer with input rows.
 * @param mergeAggregationBuffer Function to merge the aggregation buffer with input aggregation
 *                               buffer.
 * @param makeEmptyAggregationBuffer Creates an empty aggregation buffer
 */
class SortBasedAggregationStore(
    initialAggBufferSorter: UnsafeKVExternalSorter,
    inputSchema: StructType,
    groupingSchema: StructType,
    updateInputRow: (MutableRow, InternalRow) => Unit,
    mergeAggregationBuffer: (MutableRow, InternalRow) => Unit,
    makeEmptyAggregationBuffer: => MutableRow) {

  // external sorter to sort the input (grouping key + input row) with grouping key.
  private val inputSorter = createExternalSorterForInput()
  private val groupingKeyOrdering: BaseOrdering = GenerateOrdering.create(groupingSchema)

  def addInput(groupingKey: UnsafeRow, inputRow: UnsafeRow): Unit = {
    inputSorter.insertKV(groupingKey, inputRow)
  }

  /**
   * Returns a destructive iterator of AggregationBufferEntry.
   * Notice: it is illegal to call any method after `destructiveIterator()` has been called.
   */
  def destructiveIterator(): Iterator[AggregationBufferEntry] = {
    new Iterator[AggregationBufferEntry] {
      val inputIterator = inputSorter.sortedIterator()
      var hasNextInput: Boolean = next(inputIterator)

      val initialAggBufferIterator = initialAggBufferSorter.sortedIterator()
      var hasNextAggBuffer: Boolean = next(initialAggBufferIterator)

      private var result: AggregationBufferEntry = null
      private var groupingKey: UnsafeRow = null

      override def hasNext(): Boolean = {
        result != null || findNextSortGroup()
      }

      override def next(): AggregationBufferEntry = {
        val returnResult = result
        result = null
        returnResult
      }

      private def findNextSortGroup(): Boolean = {
        if (hasNextInput || hasNextAggBuffer) {
          // Find smaller key of the initialAggBufferIterator and initialAggBufferIterator
          groupingKey = findGroupingKey()
          result = new AggregationBufferEntry(groupingKey, makeEmptyAggregationBuffer)

          while (hasNextInput &&
            groupingKeyOrdering.compare(inputIterator.getKey, groupingKey) == 0) {
            updateInputRow(inputIterator.getValue, result.aggregationBuffer)
            hasNextInput = next(inputIterator)
          }

          while (hasNextAggBuffer &&
            groupingKeyOrdering.compare(initialAggBufferIterator.getKey, groupingKey) == 0) {
            mergeAggregationBuffer(initialAggBufferIterator.getValue, result.aggregationBuffer)
            hasNextAggBuffer = next(initialAggBufferIterator)
          }

          true
        } else {
          false
        }
      }

      private def next(iter: UnsafeKVExternalSorter#KVSorterIterator): Boolean = {
        val hasNext = iter.next()
        if (!hasNext) {
          iter.close()
        }
        hasNext
      }

      private def findGroupingKey(): UnsafeRow = {
        if (!hasNextInput) {
          initialAggBufferIterator.getKey
        } else if (!hasNextAggBuffer) {
          inputIterator.getKey
        } else {
          val compareResult =
            groupingKeyOrdering.compare(inputIterator.getKey, initialAggBufferIterator.getKey)
          if (compareResult <= 0) {
            inputIterator.getKey
          } else {
            initialAggBufferIterator.getKey
          }
        }
      }
    }
  }

  private def createExternalSorterForInput(): UnsafeKVExternalSorter = {
    new UnsafeKVExternalSorter(
      groupingSchema,
      inputSchema,
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      TaskContext.get().taskMemoryManager().pageSizeBytes,
      SparkEnv.get.conf.getLong(
        "spark.shuffle.spill.numElementsForceSpillThreshold",
        UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD),
      null
    )
  }
}
