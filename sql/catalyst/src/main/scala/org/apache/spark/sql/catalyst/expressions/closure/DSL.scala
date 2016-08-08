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
 * DSL to construct the Node tree
 *
 * This class does constant folding optimization during constructing of the Node tree to remove
 * unnecessary code branches that if-jump instruction creates.
 */
object DSL {
  def plus(left: Node, right: Node): Node = {
    (left, right) match {
      case (Constant(a: Int), Constant(b: Int)) => Constant(a + b)
      case (Constant(a: Float), Constant(b: Float)) => Constant(a + b)
      case (Constant(a: Long), Constant(b: Long)) => Constant(a + b)
      case (Constant(a: Double), Constant(b: Double)) => Constant(a + b)
      case _ => Arithmetic("+", left, right, left.dataType)
    }
  }

  def minus(left: Node, right: Node): Node = {
    (left, right) match {
      case (Constant(a: Int), Constant(b: Int)) => Constant(a - b)
      case (Constant(a: Float), Constant(b: Float)) => Constant(a - b)
      case (Constant(a: Long), Constant(b: Long)) => Constant(a - b)
      case (Constant(a: Double), Constant(b: Double)) => Constant(a - b)
      case _ => Arithmetic("-", left, right, left.dataType)
    }
  }

  // multiply
  def mul(left: Node, right: Node): Node = {
    (left, right) match {
      case (Constant(a: Int), Constant(b: Int)) => Constant(a * b)
      case (Constant(a: Float), Constant(b: Float)) => Constant(a * b)
      case (Constant(a: Long), Constant(b: Long)) => Constant(a * b)
      case (Constant(a: Double), Constant(b: Double)) => Constant(a * b)
      case _ => Arithmetic("*", left, right, left.dataType)
    }
  }

  // divide
  def div(left: Node, right: Node): Node = {
    (left, right) match {
      case (Constant(a: Int), Constant(b: Int)) => Constant(a / b)
      case (Constant(a: Float), Constant(b: Float)) => Constant(a / b)
      case (Constant(a: Long), Constant(b: Long)) => Constant(a / b)
      case (Constant(a: Double), Constant(b: Double)) => Constant(a / b)
      case _ => Arithmetic("/", left, right, left.dataType)
    }
  }

  // remainder
  def rem(left: Node, right: Node): Node = {
    (left, right) match {
      case (Constant(a: Int), Constant(b: Int)) => Constant(a % b)
      case (Constant(a: Float), Constant(b: Float)) => Constant(a % b)
      case (Constant(a: Long), Constant(b: Long)) => Constant(a % b)
      case (Constant(a: Double), Constant(b: Double)) => Constant(a % b)
      case _ => Arithmetic("%", left, right, left.dataType)
    }
  }

  def bitwiseAnd(left: Node, right: Node): Node = {
    (left, right) match {
      case (Constant(a: Int), Constant(b: Int)) => Constant(a & b)
      case (Constant(a: Long), Constant(b: Long)) => Constant(a & b)
      case _ => Arithmetic("&", left, right, left.dataType)
    }
  }

  def bitwiseOr(left: Node, right: Node): Node = {
    (left, right) match {
      case (Constant(a: Int), Constant(b: Int)) => Constant(a | b)
      case (Constant(a: Long), Constant(b: Long)) => Constant(a | b)
      case _ => Arithmetic("|", left, right, left.dataType)
    }
  }

  def bitwiseXor(left: Node, right: Node): Node = {
    (left, right) match {
      case (Constant(a: Int), Constant(b: Int)) => Constant(a ^ b)
      case (Constant(a: Long), Constant(b: Long)) => Constant(a ^ b)
      case _ => Arithmetic("^", left, right, left.dataType)
    }
  }

  def compareEqual(left: Node, right: Node): Node = {
    (left, right) match {
      case (Constant(a), Constant(b)) => Constant(a == b)
      case _ => Arithmetic("==", left, right, BOOLEAN_TYPE)
    }
  }

  def compareNotEqual(left: Node, right: Node): Node = {
    (left, right) match {
      case (Constant(a), Constant(b)) => Constant(!(a == b))
      case _ => Arithmetic("!=", left, right, BOOLEAN_TYPE)
    }
  }

  // less than
  def lt(left: Node, right: Node): Node = {
    (left, right) match {
      case (Constant(a: Int), Constant(b: Int)) => Constant(a < b)
      case (Constant(a: Float), Constant(b: Float)) => Constant(a < b)
      case (Constant(a: Long), Constant(b: Long)) => Constant(a < b)
      case (Constant(a: Double), Constant(b: Double)) => Constant(a < b)
      case _ => Arithmetic("<", left, right, BOOLEAN_TYPE)
    }
  }

  // greater than
  def gt(left: Node, right: Node): Node = {
    (left, right) match {
      case (Constant(a: Int), Constant(b: Int)) => Constant(a > b)
      case (Constant(a: Float), Constant(b: Float)) => Constant(a > b)
      case (Constant(a: Long), Constant(b: Long)) => Constant(a > b)
      case (Constant(a: Double), Constant(b: Double)) => Constant(a > b)
      case _ => Arithmetic(">", left, right, BOOLEAN_TYPE)
    }
  }

  // less than or equals to
  def le(left: Node, right: Node): Node = {
    (left, right) match {
      case (Constant(a: Int), Constant(b: Int)) => Constant(a <= b)
      case (Constant(a: Float), Constant(b: Float)) => Constant(a <= b)
      case (Constant(a: Long), Constant(b: Long)) => Constant(a <= b)
      case (Constant(a: Double), Constant(b: Double)) => Constant(a <= b)
      case _ => Arithmetic("<=", left, right, BOOLEAN_TYPE)
    }
  }

  // greater than or equals to
  def ge(left: Node, right: Node): Node = {
    (left, right) match {
      case (Constant(a: Int), Constant(b: Int)) => Constant(a >= b)
      case (Constant(a: Float), Constant(b: Float)) => Constant(a >= b)
      case (Constant(a: Long), Constant(b: Long)) => Constant(a >= b)
      case (Constant(a: Double), Constant(b: Double)) => Constant(a >= b)
      case _ => Arithmetic(">=", left, right, BOOLEAN_TYPE)
    }
  }

  def not(node: Node): Node = {
    node match {
      case Constant(bool: Boolean) => Constant(!bool)
      case Arithmetic(">", left, right, _) => Arithmetic("<=", left, right, BOOLEAN_TYPE)
      case Arithmetic("<", left, right, _) => Arithmetic(">=", left, right, BOOLEAN_TYPE)
      case Arithmetic(">=", left, right, _) => Arithmetic("<", left, right, BOOLEAN_TYPE)
      case Arithmetic("<=", left, right, _) => Arithmetic(">", left, right, BOOLEAN_TYPE)
      case Arithmetic("==", left, right, _) => Arithmetic("!=", left, right, BOOLEAN_TYPE)
      case Arithmetic("!=", left, right, _) => Arithmetic("==", left, right, BOOLEAN_TYPE)
      case Arithmetic("!", left, _, _) => left
      // Not is encoded as an Arithmetic with the second operand ignored
      case _ => Arithmetic("!", node, Constant(false), BOOLEAN_TYPE)
    }
  }

  def cast[T: ClassTag](node: Node): Node = {
    DSL.cast(node, getType(classTag[T].runtimeClass))
  }

  def cast(node: Node, dataType: Type): Node = dataType match {
    case dataType if dataType == node.dataType => node
    case FLOAT_TYPE =>
      node match {
        case Constant(i: Int) => Constant(i.toFloat)
        case Constant(double: Double) => Constant(double.toFloat)
        case Constant(l: Long) => Constant(l.toFloat)
        case Cast(inner, DOUBLE_TYPE) if inner.dataType == FLOAT_TYPE => inner
        case other => Cast(other, dataType)
      }
    case DOUBLE_TYPE =>
      node match {
        case Constant(i: Int) => Constant(i.toDouble)
        case Constant(float: Float) => Constant(float.toDouble)
        case Constant(l: Long) => Constant(l.toDouble)
        case other => Cast(other, dataType)
      }
    case LONG_TYPE =>
      node match {
        case Constant(i: Int) => Constant(i.toLong)
        case Constant(float: Float) => Constant(float.toLong)
        case Constant(double: Double) => Constant(double.toLong)
        case other => Cast(other, dataType)
      }
    case INT_TYPE =>
      node match {
        case Constant(bool: Boolean) => if (bool) Constant(1) else Constant(0)
        case Constant(float: Float) => Constant(float.toInt)
        case Constant(double: Double) => Constant(double.toInt)
        case Constant(long: Long) => Constant(long.toInt)
        case Constant(byte: Byte) => Constant(byte.toInt)
        case Constant(short: Short) => Constant(short.toInt)
        case Constant(char: Char) => Constant(char.toInt)
        case Cast(inner, LONG_TYPE) if inner.dataType == INT_TYPE => inner
        case other => Cast(other, dataType)
      }
    case BYTE_TYPE =>
      node match {
        case Constant(i: Int) => Constant(i.toByte)
        case Cast(inner, INT_TYPE) if inner.dataType == BYTE_TYPE => inner
        case other => Cast(other, dataType)
      }
    case SHORT_TYPE =>
      node match {
        case Constant(i: Int) => Constant(i.toShort)
        case Cast(inner, INT_TYPE) if inner.dataType == SHORT_TYPE => inner
        case other => Cast(other, dataType)
      }
    case CHAR_TYPE =>
      node match {
        case Constant(i: Int) => Constant(i.toChar)
        case Cast(inner, INT_TYPE) if inner.dataType == CHAR_TYPE => inner
        case other => Cast(other, dataType)
      }
    case BOOLEAN_TYPE =>
      node match {
        case Constant(0) => Constant(false)
        case Constant(1) => Constant(true)
        case Cast(inner, INT_TYPE) if inner.dataType == BOOLEAN_TYPE => inner
        case If(condition, left, right, _) =>
          ifElse(condition, cast(left, dataType), cast(right, dataType))
        case node =>
          Cast(node, dataType)
      }
    case _ =>
      Cast(node, dataType)
  }

  def ifElse(condition: Node, trueNode: => Node, falseNode: => Node): Node = condition match {
    case Constant(true) => trueNode
    case Constant(false) => falseNode
    case _ =>
      val trueStatement = trueNode
      val falseStatement = falseNode
      (trueStatement, falseStatement) match {
        case (Constant(true), Constant(false)) => condition
        case (Constant(false), Constant(true)) => not(condition)
        case _ if trueStatement == falseStatement => trueStatement
        case (Constant(null), right) => If(condition, Constant(null), right, right.dataType)
        case (left, Constant(null)) => If(condition, left, Constant(null), left.dataType)
        case (left, right) if left.dataType != right.dataType =>
          throw new ByteCodeParserException(
            s"If node (left = $left, right = $right) 's left branch's data type " +
              "${left.dataType} mismatches with right branch's data type ${right.dataType}.")
        case _ => If(condition, trueStatement, falseStatement, trueStatement.dataType)
      }
  }

  // Creates an Argument Node to represent the closure input argument.
  def arg[T: ClassTag]: Argument = arg(Type.getType(classTag[T].runtimeClass))

  def arg(dataType: Type): Argument = Argument(dataType)
}
