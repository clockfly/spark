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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.closure.ByteCodeParser.Node
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{StructType}

object ClosureTranslation extends Logging {
  import ExpressionGenerator._

  def translateMap(
      closure: AnyRef,
      argumentType: Class[_],
      argumentSchema: StructType)
  : Seq[Expression] = {
    try {
      val parser = new ByteCodeParser
      val node = parser.parse(closure.getClass, argumentType)
      trace(node)
      val expressionGenerator = new ExpressionGenerator
      val expressions = expressionGenerator.generate(node, argumentType, argumentSchema)
      trace(expressions)
      if (node.dataType.isAtomic && expressions.length == 1) {
        Seq(Alias(expressions(0), "value")())
      } else {
        expressions
      }
    } catch {
      case NonFatal(ex) =>
        logError(s"Failed to translate closure ${closure.getClass.getName}", ex)
        Seq.empty[Expression]
    }
  }

  def translateFilter(
      closure: AnyRef,
      argumentType: Class[_],
      argumentSchema: StructType)
    : Option[Expression] = {
    try {
      val parser = new ByteCodeParser
      val node = parser.parse(closure.getClass, argumentType)
      trace(node)
      val expressionGenerator = new ExpressionGenerator
      val expressions = expressionGenerator.generate(node, argumentType, argumentSchema)
      trace(expressions)
      if (expressions.length == 1) {
        Some(expressions(0))
      } else {
        None
      }
    } catch {
      case NonFatal(ex) =>
        logError(s"Failed to translate closure ${closure.getClass.getName}", ex)
        None
    }
  }

  def translateTypedAggregate(closure: AnyRef,
      argumentType: Class[_],
      argumentSchema: StructType)
    : Option[Expression] = {
    try {
    val parser = new ByteCodeParser
    val node = parser.parse(closure.getClass, argumentType)
    trace(node)

    val expressionGenerator = new ExpressionGenerator
    val expressions = expressionGenerator.generate(node, argumentType, argumentSchema)
    trace(expressions)
    if (expressions.length == 1) {
      Some(expressions(0))
    } else {
      None
    }
    } catch {
      case NonFatal(ex) =>
        logError(s"Failed to translate closure ${closure.getClass.getName}", ex)
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
