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

package org.apache.spark.sql.execution

import org.apache.spark.sql.ExperimentalMethods
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{CombineFilters, Optimizer, PushDownPredicate, PushPredicateThroughJoin}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.python.ExtractPythonUDFFromAggregate
import org.apache.spark.sql.internal.SQLConf

class SparkOptimizer(
    catalog: SessionCatalog,
    conf: SQLConf,
    experimentalMethods: ExperimentalMethods)
  extends Optimizer(catalog, conf) {

  override def batches: Seq[Batch] = super.batches :+
    Batch("PartitionPruning", Once,
      PartitionPruning(conf),
      OptimizeSubqueries) :+
    Batch("Pushdown pruning subquery", fixedPoint,
      PushPredicateThroughJoin,
      PushDownPredicate,
      CombineFilters) :+
    Batch("Extract Python UDF from Aggregate", Once, ExtractPythonUDFFromAggregate) :+
    Batch("User Provided Optimizers", fixedPoint, experimentalMethods.extraOptimizations: _*)
}

/**
 * Inserts a predicate for partitioned table when partition column is used as join key.
 */
case class PartitionPruning(conf: SQLConf) extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Returns whether an attribute is a partition column or not.
   */
  private def isPartitioned(a: Expression, plan: LogicalPlan): Boolean = {
    plan.foreach {
      case l: LogicalRelation if a.references.subsetOf(l.outputSet) =>
        l.relation match {
          case fs: HadoopFsRelation =>
            val partitionColumns = AttributeSet(
              l.resolve(fs.partitionSchema, fs.sparkSession.sessionState.analyzer.resolver))
            if (a.references.subsetOf(partitionColumns)) {
              return true
            }
          case _ =>
        }
      case _ =>
    }
    false
  }

  private def insertPredicate(
      partitionedPlan: LogicalPlan,
      partitioned: Expression,
      otherPlan: LogicalPlan,
      value: Expression): LogicalPlan = {
    val alias = value match {
      case a: Attribute => a
      case o => Alias(o, o.toString)()
    }
    Filter(
      PredicateSubquery(Aggregate(Seq(alias), Seq(alias), otherPlan), Seq(partitioned)),
      partitionedPlan)
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.partitionPruning) {
      return plan
    }
    plan transformUp {
      case join @ Join(left, right, joinType, Some(condition)) =>
        var newLeft = left
        var newRight = right
        splitConjunctivePredicates(condition).foreach {
          case e @ EqualTo(a: Expression, b: Expression) =>
            // they should come from different sides, otherwise should be pushed down
            val (l, r) = if (a.references.subsetOf(left.outputSet) &&
              b.references.subsetOf(right.outputSet)) {
              a -> b
            } else {
              b -> a
            }
            if (isPartitioned(l, left) && hasHighlySelectivePredicate(right) &&
              (joinType == Inner || joinType == LeftSemi || joinType == RightOuter) &&
              r.references.subsetOf(right.outputSet)) {
              newLeft = insertPredicate(newLeft, l, right, r)
            } else if (isPartitioned(r, right) && hasHighlySelectivePredicate(left) &&
              (joinType == Inner || joinType == LeftOuter) &&
              l.references.subsetOf(left.outputSet)) {
              newRight = insertPredicate(newRight, r, left, l)
            }
          case _ =>
        }
        Join(newLeft, newRight, joinType, Some(condition))
    }
  }

  /**
   * Returns whether an expression is highly selective or not.
   */
  def isHighlySelective(e: Expression): Boolean = e match {
    case Not(expr) => isHighlySelective(expr)
    case And(l, r) => isHighlySelective(l) || isHighlySelective(r)
    case Or(l, r) => isHighlySelective(l) && isHighlySelective(r)
    case _: BinaryComparison => true
    case _: In | _: InSet => true
    case _: StringPredicate => true
    case _ => false
  }

  def hasHighlySelectivePredicate(plan: LogicalPlan): Boolean = {
    plan.find {
      case f: Filter => isHighlySelective(f.condition)
      case _ => false
    }.isDefined
  }
}
