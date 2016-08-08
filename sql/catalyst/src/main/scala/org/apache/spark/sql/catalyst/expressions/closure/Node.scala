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

import scala.reflect.ClassTag
import scala.reflect.classTag

import org.apache.xbean.asm5.Type
import org.apache.xbean.asm5.Type._

/**
 * The result of closure byte code parsing is represented as a AST Node tree.
 * For example, closure:
 * {{{
 *   (v: Int) => v + 1
 * }}}
 *
 * is represented as Node tree:
 * {{{
 *   Arithmetic(
 *     "+",
 *     Argument(INT_TYPE),
 *     Constant[Int](1))
 * }}}
 */
sealed trait Node {
  def children: List[Node]
  def dataType: Type
  def treeString: String = {
    val builder = new StringBuilder
    def simpleString: PartialFunction[Node, String] = {
      case product: Node with Product =>
        val children = product.children.toSet[Any]
        val args = product.productIterator.toList.filterNot {
          case l: Iterable[_] => l.toSet.subsetOf(children)
          case e if children.contains(e) => true
          case dataType: Type => true
          case _ => false
        }
        val argString = if (args.length > 0) args.mkString("(", ", ", ")") else ""
        s"${product.getClass.getSimpleName}[${product.dataType}]$argString"
      case other => s"$other"
    }

    def buildTreeString(node: Node, depth: Int): Unit = {
      (0 until depth).foreach(_ => builder.append("  "))
      builder.append(simpleString(node))
      builder.append("\n")
      node.children.foreach(buildTreeString(_, depth + 1))
    }

    buildTreeString(this, 0)
    builder.toString()
  }
}

sealed trait BinaryNode extends Node {
  def left: Node
  def right: Node
  override def children: List[Node] = List(left, right)
}

sealed trait UnaryNode extends Node {
  def node: Node
  override def children: List[Node] = List(node)
}

sealed trait LeafNode extends Node {
  override def children: List[Node] = List.empty[Node]
}

// Represents void return type
case object VOID extends LeafNode {
  override def dataType: Type = VOID_TYPE
}

case class Constant[T: ClassTag](value: T) extends LeafNode {
  def dataType: Type = getType(classTag[T].runtimeClass)
  override def toString: String = s"$value"
}

// Represents input argument of closure
case class Argument(dataType: Type) extends LeafNode {
  override def toString: String = s"Argument"
}

// Represents the closure object.
case class This(dataType: Type) extends LeafNode {
  override def toString: String = "This"
}

// if (condition == true) left else right
case class If(condition: Node, left: Node, right: Node, dataType: Type) extends BinaryNode

// Represents function call. if it is a static function call, then obj is null.
case class FunctionCall(
  obj: Node,
  className: String,
  method: String,
  arguments: List[Node],
  dataType: Type)
  extends Node {
  def children: List[Node] = obj::arguments
  override def toString: String = {
    if (obj == null) {
      s"$className.$method(${arguments.mkString(", ")})"
    } else {
      s"$className.$method(${(obj::arguments).mkString(", ")})"
    }
  }
}

// Represents a static field access.
case class StaticField(clazz: String, name: String, dataType: Type) extends LeafNode

// Does a type cast.
case class Cast(node: Node, dataType: Type) extends UnaryNode

/**
 * Arithmetic and logical operators
 *
 * @param operator one of +, -, *, /, <, >, ==, !=, <=, >=, !
 * @param left the left operand of the operator.
 * @param right, the right operand of the operator. For operator !, the value is ignored.
 */
case class Arithmetic(
  operator: String,
  left: Node,
  right: Node,
  dataType: Type) extends BinaryNode {

  private val validOperators =
    Set("+", "-", "*", "/", "%", "<", ">", "==", "!=", "<=", ">=", "!", "&", "|", "^")
  require(validOperators.contains(operator))
  if (operator == "!") {
    require(right == Constant(false))
  }

  override def toString: String = {
    val leftString = if (left.children.length > 1) s"($left)" else s"$left"
    val rightString = if (right.children.length > 1) s"($right)" else s"$right"
    if (operator == "!") {
      s"$operator$leftString"
    } else {
      s"$leftString $operator $rightString"
    }
  }
}
