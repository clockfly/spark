/* Copyright © 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.sql.execution.closure

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.expressions.closure.ClosureTranslation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{DeserializeToObject, Filter, LogicalPlan, MapElements, Project, SerializeFromObject, TypedFilter, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.closure.TranslateClosureOptimizerRule.Parent
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AtomicType, StructField, StructType}

/**
 * Rule to translates Dataset typed operation like map[T => U] or filter[T] to untyped operation.
 *
 * NOTE: This rule must be applied immediately after the analysis stage.
 *
 * For example, typed filter operation:
 * {{{
 *   spark.conf.set("spark.sql.translateClosure", "true")
 *   val ds = (0 to 10).toDS
 *   val query = ds.filter(_ > 5)
 * }}}
 *
 * is translated to untyped filter operation:
 * {{{
 *   scala> res1.explain()
 *   == Physical Plan ==
 *   *Filter (value#1 > 5)
 *   +- LocalTableScan [value#1]
 * }}}
 *
 * Typed map operation:
 * {{{
 *
 *   spark.conf.set("spark.sql.translateClosure", "true")
 *   val ds = spark.sparkContext.parallelize((0 to 10)).toDS
 *   val query = ds2.map(_ * 2)
 * }}}
 *
 * is translated to a Project operation:
 * {{{
 *   scala> query.explain()
 *   == Physical Plan ==
 *   *Project [(value#29 * 2) AS value#34]
 *   +- *SerializeFromObject [input[0, int, true] AS value#29]
 *      +- Scan ExternalRDDScan[obj#28]
 * }}}
 *
 */
case class TranslateClosureOptimizerRule(conf: SQLConf) extends Rule[LogicalPlan] {

  private def isConfigSet: Boolean = {
    conf.getConfString(TranslateClosureOptimizerRule.CONFIG_KEY, "false") == "true"
  }

  def apply(root: LogicalPlan): LogicalPlan = {
    val rule = (parent: LogicalPlan) => parent.mapChildren {
      // Translates the MapElements to Project.
      case serializer @ SerializeFromObject(_, map @ MapElements(func, argumentType, argumentSchema,
        _, DeserializeToObject(_, _, child)))
        if shouldTranslate(map, parent, child, argumentSchema) =>
        val resolvedExpressions = ClosureTranslation
          .translateMap(func, argumentType, argumentSchema).flatMap(resolve(_, child))
        // The Project should output attributes with same exprIds as SerializeFromObject.
        renameTo(resolvedExpressions, serializer.output) match {
          case Some(expressions) => Project(expressions, child)
          case None => serializer
        }
      // Translates the TypedFilter to Filter.
      case filter @ TypedFilter(func, clazz, argumentSchema, _, child)
        if shouldTranslate(filter, parent, child, argumentSchema) =>
        val filterExpression = ClosureTranslation.translateFilter(func, clazz, argumentSchema)
        val untypedFilter = filterExpression match {
          case Some(expression) => resolve(expression, child).map(Filter(_, child))
          case None => None
        }
        untypedFilter.getOrElse(filter)
      case other => other
    }

    Parent(root).transformDown(PartialFunction(rule)).transformUp(PartialFunction(rule))
      .children(0)
  }

  // Renames expression using existing name and ExprId of newNames
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

  // Resolves all UnresolvedAttribute
  private def resolve(expr: Expression, child: LogicalPlan): Option[Expression] = {
    val afterResolve = expr.transformUp {
      case u @ UnresolvedAttribute(nameParts) =>
        child.resolve(nameParts, conf.resolver).getOrElse(u)
    }
    if (afterResolve.resolved) {
      Some(afterResolve)
    } else {
      None
    }
  }

  // Tests feature flag and does schema verification to make sure this conersion is supported.
  private def shouldTranslate(
      current: LogicalPlan,
      parent: LogicalPlan,
      child: LogicalPlan,
      argumentSchema: StructType): Boolean = {

    if (isConfigSet) {
      val isSchemaMatch = schemaMatch(child, argumentSchema)

      if (!isSchemaMatch) {
        logError(
          s"""Schema mismatch when translating closure in plan ${current.simpleString}.
             |Child plan's schema is ${child.schema},
             |argument type T's schema is ${argumentSchema}.
             |""".stripMargin)
      }
      // If parent is not typed, or child is not typed, it means current operator lies at the
      // boundary of typed and untyped operators. Then this typed operator can be safely translated
      // to untyped operator, without requiring new serialization/de-serialization operators.
      isSchemaMatch && (!isTyped(parent) || !isTyped(child))
    } else {
      false
    }
  }

  // Tests if all fields of argumentSchema can be found in logicalPlan's schema
  private def schemaMatch(child: LogicalPlan, argumentSchema: StructType): Boolean = {

    // Returns true if containee is a sub-tree of container
    def contains(container: StructField, containee: StructField): Boolean = {
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
                contains(leftField.get, rightField.get)
            }
          }
        case _ => false
      }
    }
    val logicalPlanSchema = child.schema
    contains(
      // Wrap the top level struct type in a StructField
      container = StructField("toplevel", logicalPlanSchema),
      containee = StructField("toplevel", argumentSchema))
  }

  // Checks whether this plan is a typed operation like TypedFilter
  private def isTyped(plan: LogicalPlan): Boolean = {
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
  case class Parent(child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
  }

  val CONFIG_KEY = "spark.sql.translateClosure"
}
