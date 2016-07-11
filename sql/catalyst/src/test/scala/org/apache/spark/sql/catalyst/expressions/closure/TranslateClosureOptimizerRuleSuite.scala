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

package org.apache.spark.sql.catalyst.expressions.closure

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Encoder}
import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, MapElements, Project, TypedFilter}

class TranslateClosureOptimizerRuleSuite extends SparkFunSuite {
  import TranslateClosureOptimizerRuleSuite._

  private val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  private implicit def encoder[T: TypeTag] = ExpressionEncoder()

  private def create(closureTranslation: Boolean): TranslateClosureOptimizerRule = {
    val conf = new SimpleCatalystConf(false, closureTranslation)
    TranslateClosureOptimizerRule(conf)
  }

  test("translation disabled by setting a config") {
    val optimizer = create(closureTranslation = false)
    val query = testRelation.filter[A](_.a > 0)
    // closureTranslation is disabled by config entry spark.sql.translateClosureToExpression
    assert(optimizer.apply(query) == query)
  }


  test("translation disabled for unsupported closure type") {
    val optimizer = create(closureTranslation = true)
    val query = testRelation.filter[A](x => Math.sqrt(x.a) > 0)
    // closureTranslation is disabled as Math.sqrt is not supported.
    assert(optimizer.apply(query) == query)
  }

  test("translation disabled if logical plan's schema mismatch with argument type T's schema") {
    val optimizer = create(closureTranslation = true)
    val query = testRelation.filter[B](x => x.a > 0)
    // closureTranslation is disabled because schema mismatch.
    // B.a is of type Double, while testRelation.a is of type Integer
    assert(optimizer.apply(query) == query)
  }

  test("translates typed filter to untyped filter") {
    val optimizer = create(closureTranslation = true)
    val query = testRelation.filter[A](_.a > 0)
    val optimized = optimizer.apply(query)
    // Translate TypedFilter to Filter
    optimized match {
      case Filter(_, LocalRelation(_, _)) => // pass
      case _ => fail
    }
  }

  test("translates typed map to untyped map") {
    val optimizer = create(closureTranslation = true)
    val query = testRelation.typedMap[A, Int](_.a * 2)
    val optimized = optimizer.apply(query)
    optimized match {
      case Project(_, LocalRelation(_, _)) => // pass
      case _ => fail
    }
  }

  test("translates a chain of typed operators") {
    val optimizer = create(closureTranslation = true)
    val query = testRelation
      .typedMap[A, Int](_.a * 2)
      .filter[Int](_ > 0)
      // Math.sqrt cannot be optimized by closure translation
      .filter[Int](x => Math.sqrt(x) < 10)
      .filter[Int](_ < 5)

    val optimized = optimizer.apply(query)
    optimized match {
      case Filter(_,
        TypedFilter(_, _, _, _,
          Filter(_,
            Project(_,
              LocalRelation(_, _))))) => // pass
      case _ => fail
    }
  }
}

object TranslateClosureOptimizerRuleSuite {
  case class A(a: Int, b: Int)

  case class B(a: Double)

  implicit class MapLogicalPlan(plan: LogicalPlan) {
    def typedMap[T <: Product: Encoder, U: Encoder](func: T => U): LogicalPlan = {
      MapElements[T, U](func, plan)
    }
  }
}
