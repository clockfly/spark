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

import scala.util.Random

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveSessionCatalog
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

class ObjectHashAggregateSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import testImplicits._

  test("typed_count without grouping keys") {
    val df = Seq((1: Integer, 2), (null, 2), (3: Integer, 4)).toDF("a", "b")

    checkAnswer(
      df.coalesce(1).select(typed_count($"a")),
      Seq(Row(2))
    )
  }

  test("typed_count without grouping keys and empty input") {
    val df = Seq.empty[(Integer, Int)].toDF("a", "b")

    checkAnswer(
      df.coalesce(1).select(typed_count($"a")),
      Seq(Row(0))
    )
  }

  test("typed_count with grouping keys") {
    val df = Seq((1: Integer, 1), (null, 1), (2: Integer, 2)).toDF("a", "b")

    checkAnswer(
      df.coalesce(1).groupBy($"b").agg(typed_count($"a")),
      Seq(
        Row(1, 1),
        Row(2, 1))
    )
  }

  test("typed_count fallback to sort-based aggregation") {
    withSQLConf(SQLConf.OBJECT_AGG_FALLBACK_COUNT_THRESHOLD.key -> "2") {
      val df = Seq(
        (null, 1),
        (null, 1),
        (1: Integer, 1),
        (2: Integer, 2),
        (2: Integer, 2),
        (2: Integer, 2)
      ).toDF("a", "b")

      checkAnswer(
        df.coalesce(1).groupBy($"b").agg(typed_count($"a")),
        Seq(Row(1, 1), Row(2, 3))
      )
    }
  }

  test("random input data types") {
    val dataTypes = Seq(
      // Integral types
      ByteType, ShortType, IntegerType, LongType,

      // Fractional types
      FloatType, DoubleType,

      // Decimal types
      DecimalType(25, 5), DecimalType(6, 5),

      // Datetime types
      DateType, TimestampType,

      // Complex types
      ArrayType(IntegerType),
      MapType(DoubleType, LongType),
      new StructType()
        .add("f1", FloatType, nullable = true)
        .add("f2", ArrayType(BooleanType), nullable = true),

      // UDT
      new UDT.MyDenseVectorUDT(),

      // Others
      StringType,
      BinaryType, NullType, BooleanType
    )

    dataTypes.sliding(2, 1).map(_.toSeq).foreach { dataTypes =>
      // Schema used to generate random input data.
      val schemaForGenerator = StructType(dataTypes.zipWithIndex.map {
        case (fieldType, index) =>
          StructField(s"col_$index", fieldType, nullable = true)
      })

      // Schema of the DataFrame to be tested.
      val schema = StructType(
        StructField("id", IntegerType, nullable = false) +: schemaForGenerator.fields
      )

      logInfo(s"Testing schema:\n${schema.treeString}")

      // Creates a DataFrame for the schema with random data.
      val data = generateRandomRows(schemaForGenerator)
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)
      val aggFunctions = schema.fieldNames.map(f => typed_count(col(f)))

      checkAnswer(
        df.agg(aggFunctions.head, aggFunctions.tail: _*),
        Row.fromSeq(data.map(_.toSeq).transpose.map(_.count(_ != null): Long))
      )

      checkAnswer(
        df.groupBy($"id" % 4 as 'mod).agg(aggFunctions.head, aggFunctions.tail: _*),
        data.groupBy(_.getInt(0) % 4).map { case (key, value) =>
          key -> Row.fromSeq(value.map(_.toSeq).transpose.map(_.count(_ != null): Long))
        }.toSeq.map {
          case (key, value) => Row.fromSeq(key +: value.toSeq)
        }
      )

      withSQLConf(SQLConf.OBJECT_AGG_FALLBACK_COUNT_THRESHOLD.key -> "5") {
        checkAnswer(
          df.agg(aggFunctions.head, aggFunctions.tail: _*),
          Row.fromSeq(data.map(_.toSeq).transpose.map(_.count(_ != null): Long))
        )
      }
    }
  }

  private def typed_count(column: Column): Column =
    Column(TestingTypedCount(column.expr).toAggregateExpression())

  // Generates 50 random rows for a given schema.
  private def generateRandomRows(schemaForGenerator: StructType): Seq[Row] = {
    val dataGenerator = RandomDataGenerator.forType(
      dataType = schemaForGenerator,
      nullable = true,
      new Random(System.nanoTime())
    ).getOrElse {
      fail(s"Failed to create data generator for schema $schemaForGenerator")
    }

    (1 to 50).map { i =>
      dataGenerator() match {
        case row: Row => Row.fromSeq(i +: row.toSeq)
        case null => Row.fromSeq(i +: Seq.fill(schemaForGenerator.length)(null))
        case other => fail(
          s"Row or null is expected to be generated, " +
            s"but a ${other.getClass.getCanonicalName} is generated."
        )
      }
    }
  }

  makeRandomizedTests()

  private def makeRandomizedTests(): Unit = {
    // A TypedImperativeAggregate function
    val typed = typed_count($"c0")

    // A Hive UDAF without partial aggregation support
    val withoutPartial = {
      registerHiveFunction("hive_max", classOf[GenericUDAFMax])
      function("hive_max", $"c1")
    }

    // A Spark SQL native aggregate function with partial aggregation support
    val withPartial = max($"c2")

    // A Spark SQL native distinct aggregate function
    val withDistinct = countDistinct($"c3")

    val allAggs = Seq(
      "typed" -> typed,
      "w/o partial" -> withoutPartial,
      "w/ partial" -> withPartial,
      "w/ distinct" -> withDistinct
    )

    // The schema for the randomized data generator
    val schema = new StructType()
      .add("c0", ByteType, nullable = true)
      .add("c1", ShortType, nullable = true)
      .add("c2", IntegerType, nullable = true)
      .add("c3", LongType, nullable = true)

    // Builds a randomly generated DataFrame
    val schemaWithId = StructType(StructField("id", IntegerType, nullable = false) +: schema.fields)
    val data = generateRandomRows(schema)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schemaWithId)

    // Tests all combinations of length 1, 2, 3, and 4 types of aggregate functions
    (1 to allAggs.length) foreach { i =>
      allAggs.combinations(i) foreach { targetAggs =>
        val (names, aggs) = targetAggs.unzip

        // Tests aggregation of w/ and w/o grouping keys
        Seq(true, false).foreach { withGroupingKeys =>

          // Tests aggregation with empty and non-empty input rows
          Seq(true, false).foreach { emptyInput =>

            // Builds the aggregation to be tested according to different configurations
            def doAggregation(df: DataFrame): DataFrame = {
              val baseDf = if (emptyInput) {
                val emptyRows = spark.sparkContext.parallelize(Seq.empty[Row], 1)
                spark.createDataFrame(emptyRows, schemaWithId)
              } else {
                df
              }

              if (withGroupingKeys) {
                baseDf
                  .groupBy($"id" % 10 as "group")
                  .agg(aggs.head, aggs.tail: _*)
                  .orderBy("group")
              } else {
                baseDf.agg(aggs.head, aggs.tail: _*)
              }
            }

            // Currently Spark SQL doesn't support evaluating distinct aggregate function together
            // with aggregate functions without partial aggregation support.
            if (!(aggs.contains(withoutPartial) && aggs.contains(withDistinct))) {
              test(
                s"randomized aggregation test - " +
                  s"${names.mkString("[", ", ", "]")} - " +
                  s"${if (withGroupingKeys) "w/" else "w/o"} grouping keys - " +
                  s"w/ ${if (emptyInput) "empty" else "non-empty"} input"
              ) {
                var expected: Seq[Row] = null
                var actual1: Seq[Row] = null
                var actual2: Seq[Row] = null

                // Disables `ObjectHashAggregateExec` to obtain a standard answer
                withSQLConf(SQLConf.USE_OBJECT_AGG_EXEC.key -> "false") {
                  expected = doAggregation(df).collect().toSeq
                }

                // Enables `ObjectHashAggregateExec` but disables sort-based aggregation fallback
                // (we only generate 50 rows) to obtain a result to be checked.
                withSQLConf(
                  SQLConf.USE_OBJECT_AGG_EXEC.key -> "true",
                  SQLConf.OBJECT_AGG_FALLBACK_COUNT_THRESHOLD.key -> "100"
                ) {
                  actual1 = doAggregation(df).collect().toSeq
                }

                // Enables `ObjectHashAggregateExec` and sort-based aggregation fallback to obtain
                // another result to be checked.
                withSQLConf(
                  SQLConf.USE_OBJECT_AGG_EXEC.key -> "true",
                  SQLConf.OBJECT_AGG_FALLBACK_COUNT_THRESHOLD.key -> "3"
                ) {
                  actual2 = doAggregation(df).collect().toSeq
                }

                assertResult(expected) { actual1 }
                assertResult(expected) { actual2 }
              }
            }
          }
        }
      }
    }
  }

  private def registerHiveFunction(functionName: String, clazz: Class[_]): Unit = {
    val sessionCatalog = spark.sessionState.catalog.asInstanceOf[HiveSessionCatalog]
    val builder = sessionCatalog.makeFunctionBuilder(functionName, clazz.getName)
    val info = new ExpressionInfo(clazz.getName, functionName)
    sessionCatalog.createTempFunction(functionName, info, builder, ignoreIfExists = false)
  }

  private def function(name: String, args: Column*): Column = {
    Column(UnresolvedFunction(FunctionIdentifier(name), args.map(_.expr), isDistinct = false))
  }

  private def distinctFunction(name: String, args: Column*): Column = {
    Column(UnresolvedFunction(FunctionIdentifier(name), args.map(_.expr), isDistinct = true))
  }
}
