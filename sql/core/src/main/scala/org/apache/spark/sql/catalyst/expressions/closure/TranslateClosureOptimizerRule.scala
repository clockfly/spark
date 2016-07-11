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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Count, Sum}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AppendColumns, DeserializeToObject, Filter, LogicalPlan, MapElements, Project, SerializeFromObject, TypedFilter, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{TypedAggregateExpression, TypedAverage, TypedCount, TypedSumDouble, TypedSumLong}
import org.apache.spark.sql.types.{AtomicType, StructField, StructType}

/**
 * Translates typed operation like map[T => U] or filter[T] to untyped operation.
 */
case class TranslateClosureOptimizerRule(conf: CatalystConf) extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalyst.expressions.closure.TranslateClosureOptimizerRule.Parent

  def apply(root: LogicalPlan): LogicalPlan = {

    val rule = (parent: LogicalPlan) => parent.mapChildren {
      // Translates the MapElements to Project.
      case serializer@ SerializeFromObject(_, map@ MapElements(func, argumentType, argumentSchema,
        _, DeserializeToObject(_, _, child)))
          if shouldTranslate(map, parent, child, argumentSchema) =>
        val resolvedExpressions = ClosureTranslation
          .translateMap(func, argumentType, argumentSchema).flatMap(resolve(_, child))
        // The Project should output attributes with same exprIds as SerializeFromObject.
        renameTo(resolvedExpressions, serializer.output) match {
          case Some(exprs) => Project(exprs, child)
          case None => serializer
        }
      // Translates the TypedFilter to Filter.
      case filter@ TypedFilter(func, clazz, argumentSchema, _, child)
        if shouldTranslate(filter, parent, child, argumentSchema) =>
        val filterExpression = ClosureTranslation.translateFilter(func, clazz, argumentSchema)
        val untypedFilter = filterExpression match {
          case Some(expression) => resolve(expression, child).map(Filter(_, child))
          case None => None
        }
        untypedFilter.getOrElse(filter)
      case append@ AppendColumns(f, argumentClass, argumentSchema, _, _, child)
        if shouldTranslate(append, parent, child, argumentSchema) =>
        val resolvedExpressions = ClosureTranslation.translateMap(f, argumentClass, argumentSchema)
          .flatMap(resolve(_, child))

        // The Project should output attributes with same exprIds as SerializeFromObject.
        renameTo(resolvedExpressions, append.newColumns) match {
          case Some(exprs) => Project(child.output ++ exprs, child)
          case None => append
        }
      case agg@ Aggregate(_, _, child) if conf.closureTranslation =>
        agg.transformExpressions {
          case typed: TypedAggregateExpression =>
            translateToUnTyped(typed, child).getOrElse(typed)
        }
      case other => other
    }

    Parent(root).transformDown(PartialFunction(rule)).transformUp(PartialFunction(rule))
      .children(0)
  }

  private def translateToUnTyped(
      typed: TypedAggregateExpression,
      child: LogicalPlan)
    : Option[Expression] = {
    val inputClass = typed.inputClass.get
    val inputSchema = typed.inputEncoderSchema.get
    val isSchemaMatch = schemaMatch(child, inputSchema)
    if (!isSchemaMatch) {
      logError(
        s"Schema mismatch when translating closure in expression ${typed.aggregator}. Child " +
          s"plan's schema is ${child.schema}, while input argument type T's schema " +
          s"is ${inputSchema}.")
      None
    } else {
      typed.aggregator.getClass match {
        case clazz if clazz == classOf[TypedSumDouble[_]] =>
          val sum = typed.aggregator.asInstanceOf[TypedSumDouble[_]]
          ClosureTranslation.translateTypedAggregate(sum.f, inputClass, inputSchema)
            .map(Sum(_)).flatMap(resolve(_, child))
        case clazz if clazz == classOf[TypedSumLong[_]] =>
          val sum = typed.aggregator.asInstanceOf[TypedSumLong[_]]
          ClosureTranslation.translateTypedAggregate(sum.f, inputClass, inputSchema)
            .map(Sum(_)).flatMap(resolve(_, child))
        case clazz if clazz == classOf[TypedCount[_]] =>
          val count = typed.aggregator.asInstanceOf[TypedCount[_]]
          ClosureTranslation.translateTypedAggregate(count.f, inputClass, inputSchema)
            .map(Count(_)).flatMap(resolve(_, child))
        case clazz if clazz == classOf[TypedAverage[_]] =>
          val avg = typed.aggregator.asInstanceOf[TypedAverage[_]]
          ClosureTranslation.translateTypedAggregate(avg.f, inputClass, inputSchema)
            .map(Average(_)).flatMap(resolve(_, child))
      }
    }
  }

  private def renameTo(exprs: Seq[Expression], newNames: Seq[Attribute]): Option[Seq[Alias]] = {
    if (exprs.length != newNames.length) {
      None
    } else {
      Some(exprs.zip(newNames).map(kv => renameTo(kv._1, kv._2)))
    }
  }

  private def renameTo(expr: Expression, newName: Attribute): Alias = expr match {
    case a: Alias =>
      Alias(a.child, newName.name)(newName.exprId, a.qualifier, a.explicitMetadata,
        a.isGenerated)
    case expr: Expression =>
      Alias(expr, newName.name)(exprId = newName.exprId)
  }

  private def resolve(expr: Expression, child: LogicalPlan): Option[Expression] = {
    val afterResolve = expr.transformUp {
      case u@ UnresolvedAttribute(nameParts) =>
        child.resolve(nameParts, conf.resolver).getOrElse(u)
    }
    if (afterResolve.resolved) {
      Some(afterResolve)
    } else {
      None
    }
  }

  private def shouldTranslate(
      current: LogicalPlan,
      parent: LogicalPlan,
      child: LogicalPlan,
      argumentSchema: StructType): Boolean = {

    if (conf.closureTranslation) {
      val isSchemaMatch = schemaMatch(child, argumentSchema)

      if (!isSchemaMatch) {
        logError(
          s"Schema mismatch when translating closure in plan ${current.simpleString}. Child " +
            s"plan's schema is ${child.schema}, while argument type T's schema " +
            s"is ${argumentSchema}.")
      }
      isSchemaMatch && (!typed(parent) || !typed(child))
    } else {
      false
    }
  }

  private def schemaMatch(
      child: LogicalPlan,
      argumentSchema: StructType): Boolean = {

    def fieldContains(container: StructField, containee: StructField): Boolean = {
      (container.dataType, containee.dataType) match {
        case (l: AtomicType, r: AtomicType) =>
          l == r && container.name == containee.name
        case (l: StructType, r: StructType) =>
          r.fieldNames.forall { fieldName =>
            if (l.fieldNames.count(_ == fieldName) != 1) {
              // Ambiguous field name
              false
            } else {
              val leftField = l.getFieldIndex(fieldName).map(l.fields(_))
              val rightField = r.getFieldIndex(fieldName).map(r.fields(_))
              leftField.isDefined &&
                rightField.isDefined &&
                fieldContains(leftField.get, rightField.get)
            }
          }
        case _ => false
      }
    }
    fieldContains(StructField("", child.schema), StructField("", argumentSchema))
  }

  // Checks whether this plan is a typed operation like TypedFilter
  private def typed(plan: LogicalPlan): Boolean = {
    plan match {
      case _: SerializeFromObject => true
      case _: DeserializeToObject => true
      case _: TypedFilter => true
      case _ => false
    }
  }
}

object TranslateClosureOptimizerRule {

  // A helper class for traversing the logical plan tree.
  private [TranslateClosureOptimizerRule]
  case class Parent(child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
  }
}
