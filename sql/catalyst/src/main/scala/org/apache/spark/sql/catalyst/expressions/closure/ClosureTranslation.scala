/* Copyright © 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.sql.catalyst.expressions.closure

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.closure.TypeOps.typeToTypeOps
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{StructType}

/**
 * Facade class to translates closure in Dataset typed map or typed filter operation to
 * Spark sql expression(s).
 */
object ClosureTranslation extends Logging {

  /**
   * Translates closure used in Dataset API typed map operation to Spark sql expression(s).
   *
   * For example:
   *
   * Closure:
   * {{{
   *   _ * 2
   * }}}
   *
   * in Dataset typed map operation:
   * {{{
   *   val ds = (0 to 10).toDS
   *   ds.map(_ * 2).show
   * }}}
   *
   * is translated to expression:
   * {{{
   *   Multiply(UnresolvedAttribute("value"), Literal(2))
   * }}}
   *
   * @param closure A function object of single input argument and single return value.
   * @argumentClass The closure input argument's class. For the closure in above example, the
   *               argument class is classOf[Int].
   * @argumentSchema The closure input argument's schema.
   */
  def translateMap(
      closure: AnyRef,
      argumentClass: Class[_],
      argumentSchema: StructType): Seq[Expression] = {
    try {
      val parser = new ByteCodeParser
      val node = parser.parse(closure.getClass, argumentClass)
      trace(node)
      val expressionGenerator = new ExpressionGenerator
      val expressions = expressionGenerator.generate(node, argumentClass, argumentSchema)
      trace(expressions)
      if (node.dataType.isAtomicType && expressions.length == 1) {
        Seq(Alias(expressions(0), "value")())
      } else {
        expressions
      }
    } catch {
      case NonFatal(ex) =>
        logInfo(s"Failed to translate closure ${closure.getClass.getName}", ex)
        Seq.empty[Expression]
    }
  }

  /**
   * Translates closure used in Dataset API typed filter operation to expression.
   *
   * For example:
   *
   * Closure in typed filter:
   * {{{
   *   _ > 5
   * }}}
   *
   * in Dataset typed filter operation:
   * {{{
   *   val ds = (0 to 10).toDS
   *   ds.filter(_ > 5).show
   * }}}
   *
   * is translated to expression:
   * {{{
   *   GreaterThan(UnresolvedAttribute("value"), Literal(5))
   * }}}
   *
   * @param closure A function object of single input argument and boolean return value.
   * @argumentClass The closure input argument's class. For the closure in above example, the
   *               argument class is classOf[Int].
   * @argumentSchema The closure input argument's schema.
   */
  def translateFilter(
      closure: AnyRef,
      argumentClass: Class[_],
      argumentSchema: StructType): Option[Expression] = {
    try {
      val parser = new ByteCodeParser
      val node = parser.parse(closure.getClass, argumentClass)
      trace(node)
      val expressionGenerator = new ExpressionGenerator
      val expressions = expressionGenerator.generate(node, argumentClass, argumentSchema)
      trace(expressions)
      if (expressions.length == 1) {
        Some(expressions(0))
      } else {
        None
      }
    } catch {
      case NonFatal(ex) =>
        logInfo(s"Failed to translate closure ${closure.getClass.getName}", ex)
        None
    }
  }

  private def trace(node: Node): Unit = {
    logTrace(
      s"""
         |ByteCode tree after parsing:
         |===============================================
         |${node.treeString}
       """.stripMargin)
  }

  private def trace(expressions: Seq[Expression]): Unit = {
    logTrace(
      s"""
         |Expression tree after parsing:
         |===============================================
         |${expressions.zipWithIndex.map(kv => s"Expression ${kv._2}: ${kv._1}")}
       """.stripMargin)
  }
}
