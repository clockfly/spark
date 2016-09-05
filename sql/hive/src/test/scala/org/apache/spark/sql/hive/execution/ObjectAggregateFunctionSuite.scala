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

import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

class ObjectAggregateFunctionSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
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

  test("randomized test") {
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

      val dataGenerator = RandomDataGenerator.forType(
        dataType = schemaForGenerator,
        nullable = true,
        new Random(System.nanoTime())
      ).getOrElse {
        fail(s"Failed to create data generator for schema $schemaForGenerator")
      }

      val data = (1 to 50).map { i =>
        dataGenerator() match {
          case row: Row => Row.fromSeq(i +: row.toSeq)
          case null => Row.fromSeq(i +: Seq.fill(schemaForGenerator.length)(null))
          case other => fail(
            s"Row or null is expected to be generated, " +
              s"but a ${other.getClass.getCanonicalName} is generated."
          )
        }
      }

      // Creates a DataFrame for the schema with random data.
      val rdd = spark.sparkContext.parallelize(data, 1)
      val df = spark.createDataFrame(rdd, schema)
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
}
