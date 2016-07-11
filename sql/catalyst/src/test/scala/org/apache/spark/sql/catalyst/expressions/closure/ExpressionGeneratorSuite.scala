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
import scala.reflect.ClassTag
import scala.reflect.classTag

import org.apache.xbean.asm5.Type
import org.apache.xbean.asm5.Type.{BOOLEAN_TYPE, BYTE_TYPE, CHAR_TYPE, DOUBLE_TYPE, FLOAT_TYPE, INT_TYPE, LONG_TYPE, SHORT_TYPE, _}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Add, BitwiseAnd, BitwiseOr, BitwiseXor, Cast, Divide, EqualTo, Expression, GetStructField, GreaterThan, GreaterThanOrEqual, If => IfExpression, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Multiply, Not, Remainder, Subtract}
import org.apache.spark.sql.catalyst.expressions.closure.ByteCodeParser.{Arithmetic, Constant, FunctionCall, Node, Static, This}
import org.apache.spark.sql.catalyst.expressions.closure.ByteCodeParser.DSL.{arg, _}
import org.apache.spark.sql.catalyst.expressions.closure.ExpressionGenerator._
import org.apache.spark.sql.catalyst.expressions.closure.ExpressionGeneratorSuite.{A, B}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.types.{AtomicType, DoubleType, StructField, StructType}

class ExpressionGeneratorSuite extends SparkFunSuite {

  private def arithmetic[T: ClassTag: TypeTag](constant: Constant[T]): Unit = {
    assertEqual(
      expression[T](bitwiseAnd(arg[T], constant)),
      BitwiseAnd(UnresolvedAttribute("value"), literal(constant.value)))

    assertEqual(
      expression[T](bitwiseXor(arg[T], constant)),
      BitwiseXor(UnresolvedAttribute("value"), literal(constant.value)))

    assertEqual(
      expression[T](bitwiseOr(arg[T], constant)),
      BitwiseOr(UnresolvedAttribute("value"), literal(constant.value)))

    assertEqual(
      expression[T](plus(arg[T], constant)),
      Add(UnresolvedAttribute("value"), literal(constant.value)))

    assertEqual(
      expression[T](minus(arg[T], constant)),
      Subtract(UnresolvedAttribute("value"), literal(constant.value)))

    assertEqual(
      expression[T](mul(arg[T], constant)),
      Multiply(UnresolvedAttribute("value"), literal(constant.value)))

    classTag[T].runtimeClass match {
      case x if x == classOf[Double] =>
        // Should not cast the div operand to Double if the input type is already Double
        assertEqual(
          expression[T](div(arg[T], constant)),
          Divide(
            UnresolvedAttribute("value"),
            literal(constant.value)))
      case x if x == classOf[Char] =>
        // Catalyst doesn't have Char type, we need to simulate Char with (integer & 0xfffff)
        assertEqual(
          expression[T](div(arg[T], constant)),
          BitwiseAnd(
            Cast(
              Divide(
                Cast(UnresolvedAttribute("value"), DoubleType),
                Cast(literal(constant.value), DoubleType)),
              constant.dataType.sqlType),
            Literal(0xffff)
          )
        )
      case _ =>
        assertEqual(
          expression[T](div(arg[T], constant)),
          Cast(
            Divide(
              Cast(UnresolvedAttribute("value"), DoubleType),
              Cast(literal(constant.value), DoubleType)),
            constant.dataType.sqlType))
    }

    assertEqual(
      expression[T](rem(arg[T], constant)),
      Remainder(UnresolvedAttribute("value"), literal(constant.value)))
    assertEqual(
      expression[T](compareEqual(arg[T], constant)),
      EqualTo(UnresolvedAttribute("value"), literal(constant.value)))

    assertEqual(
      expression[T](compareNotEqual(arg[T], constant)),
      Not(EqualTo(UnresolvedAttribute("value"), literal(constant.value))))

    assertEqual(
      expression[T](le(arg[T], constant)),
      LessThanOrEqual(UnresolvedAttribute("value"), literal(constant.value)))

    assertEqual(
      expression[T](lt(arg[T], constant)),
      LessThan(UnresolvedAttribute("value"), literal(constant.value)))

    assertEqual(
      expression[T](gt(arg[T], constant)),
      GreaterThan(UnresolvedAttribute("value"), literal(constant.value)))

    assertEqual(
      expression[T](ge(arg[T], constant)),
      GreaterThanOrEqual(UnresolvedAttribute("value"), literal(constant.value)))

    assertEqual(
      expression[T](not(ge(arg[T], constant))),
      LessThan(UnresolvedAttribute("value"), literal(constant.value)))
  }

  test("arithmetic expressions") {
    arithmetic[Int](Constant(4))
    arithmetic[Long](Constant(4L))
    arithmetic[Float](Constant(4.0F))
    arithmetic[Double](Constant(4.0D))
    arithmetic[Byte](Constant(4.toByte))
    arithmetic[Short](Constant(4.toShort))
    arithmetic[Char](Constant(4.toChar))
  }

  test("converts x == null to isNull(x)") {
    assertEqual(
      expression[String](compareEqual(arg[String], Constant(null))),
      IsNull(UnresolvedAttribute("value")))

    assertEqual(
      expression[String](compareEqual(Constant(null), arg[String])),
      IsNull(UnresolvedAttribute("value")))

    assertEqual(
      expression[String](compareNotEqual(arg[String], Constant(null))),
      IsNotNull(UnresolvedAttribute("value")))

    assertEqual(
      expression[String](compareNotEqual(Constant(null), arg[String])),
      IsNotNull(UnresolvedAttribute("value")))
  }

  test("cast expressions") {
    // casting between primitive types
    val casts = TypeOps.primitives.combinations(2).toList.map(pair => (pair(1), pair(0)))
    (casts ++ casts.map(_.swap)).map { pair =>
      val (left, right) = pair
      implicit val classTag = left.classTag
      if (right == CHAR_TYPE) {
        assertEqual(
          expression(cast(arg, right)),
          BitwiseAnd(Cast(UnresolvedAttribute("value"), right.sqlType), Literal(0xffff)))
      } else {
        assertEqual(
          expression(cast(arg, right)),
          Cast(UnresolvedAttribute("value"), right.sqlType))
      }
    }

    // Casting when doing boxing, no need to insert a NPE check
    TypeOps.primitives.foreach { dataType =>
      implicit val classTag = dataType.classTag
      assertEqual(
        expression(cast(arg, dataType.boxType.get)),
        UnresolvedAttribute("value" :: Nil))
    }

    // Casting when doing unboxing, need to insert a NPE check
    TypeOps.primitives.foreach { dataType =>
      implicit val classTag = dataType.boxType.get.classTag
      assertEqual(
        expression(cast(arg, dataType)),
        NPEOnNull(UnresolvedAttribute("value" :: Nil), UnresolvedAttribute("value" :: Nil)))
    }

    // Casting null
    assertEqual(
      expression[Int](cast(Constant(null), Type.getType(classOf[String]))),
      Literal(null))

    // Casting a child class object to parent class reference
    assertEqual(
      expression[String](cast(arg[String], Type.getType(classOf[AnyRef]))),
      UnresolvedAttribute("value" :: Nil))

    // Invalid castings
    val ex = intercept[ClosureTranslationException](
      expression[String](cast(arg[String], Type.getType(classOf[java.lang.Integer])))
    ).getMessage
    assert(ex.contains("Cannot cast expression 'value(Ljava/lang/String;) to Ljava/lang/Integer"))
  }

  test("closure that returns void is not supported") {
    val ex = intercept[ClosureTranslationException](
      expression[Int](ByteCodeParser.VOID)).getMessage
    assert(ex.contains("VOID expression is not supported"))
  }

  test("closure that references free variable like THIS pointer is not supported") {
    val ex = intercept[ClosureTranslationException](
      expression[String](This(Type.getType(classOf[A])))).getMessage
    assert(ex.contains("Closure that reference free variable like THIS pointer is not supported"))
  }

  test("literal expression") {
    assertEqual(expression[Boolean](Constant(false)), Literal(false))
    assertEqual(expression[Byte](Constant(33.toByte)), Literal(33.toByte))
    assertEqual(expression[Char](Constant(65626.toChar)), Literal(65626.toInt & 0xffff))
    assertEqual(expression[Short](Constant(33.toShort)), Literal(33.toShort))
    assertEqual(expression[Int](Constant(33)), Literal(33))
    assertEqual(expression[Long](Constant(33.toLong)), Literal(33.toLong))
    assertEqual(expression[Float](Constant(33.3F)), Literal(33.3F))
    assertEqual(expression[Double](Constant(33.3D)), Literal(33.3D))
  }

  test("if expression") {
    assertEqual(
      expression[Boolean](ifElse(arg[Boolean], Constant(3), Constant(7))),
      IfExpression(UnresolvedAttribute("value"), Literal(3), Literal(7))
    )
  }

  test("field projection expression like _.a.b") {
    // Projects primitive types
    TypeOps.primitives.foreach { dataType =>
      implicit val classTag = dataType.classTag
      assertEqual(
        expression(arg),
        UnresolvedAttribute("value" :: Nil)
      )
    }

    // Projects and flattens sub-fields of a case class
    assertEqual(
      generate[A](arg[A], schemaFor[A]),
      Seq(UnresolvedAttribute("a1"), UnresolvedAttribute("a2"))
    )

    // Projects and flattens the return value of an If-expression.
    val expressions = generate[A](
      ifElse(Constant(true),
        FunctionCall(arg[B], classOf[B].getName, "b1", List.empty[Node], getType(classOf[A])),
        FunctionCall(arg[B], classOf[B].getName, "b2", List.empty[Node], getType(classOf[A]))),
      schemaFor[B])
    expressions.toList match {
      case GetStructField(_, _, Some("a1")) :: GetStructField(_, _, Some("a2")) :: Nil => // pass
      case _ => fail
    }
  }

  test("static function call") {

    val predefUnboxingMethods: List[(ClassTag[_], String, Type)] = List(
      (classTag[java.lang.Long], "Long2long", LONG_TYPE),
      (classTag[java.lang.Float], "Float2float", FLOAT_TYPE),
      (classTag[java.lang.Double], "Double2double", DOUBLE_TYPE),
      (classTag[java.lang.Boolean], "Boolean2boolean", BOOLEAN_TYPE),
      (classTag[java.lang.Short], "Short2short", SHORT_TYPE),
      (classTag[java.lang.Byte], "Byte2byte", BYTE_TYPE),
      (classTag[java.lang.Integer], "Integer2int", INT_TYPE)
    )

    val predefClass = Predef.getClass
    val predefClassName = predefClass.getName

    // Valid static method call
    predefUnboxingMethods.foreach { classAndMethod =>
      implicit val classTag = classAndMethod._1
      val (_, method, dataType) = classAndMethod
      assertEqual(
        expression(
          FunctionCall(
            Static(predefClassName, "MODULE$", Type.getType(predefClass)),
            predefClassName,
            method,
            List(arg),
            dataType)),
        NPEOnNull(
          UnresolvedAttribute("value"),
          UnresolvedAttribute("value")))
    }

    // Invalid static method call
    val ex = intercept[ClosureTranslationException](
      expression[Long](
        FunctionCall(
          Constant(null),
          "java.lang.Math",
          "sqrt",
          List(arg[Double]),
          DOUBLE_TYPE))).getMessage
    assert(ex.contains("Unsupported static function"))
  }

  test("getter function call") {
    // Valid getter
    assertEqual(
      expression[A](
        FunctionCall(
          arg[A],
          classOf[A].getName,
          "a1",
          List.empty[Node],
          INT_TYPE)),
      UnresolvedAttribute("a1"))

    // Invalid getter
    val ex = intercept[ClosureTranslationException] {
      expression[A](
        FunctionCall(
          arg[A],
          classOf[A].getName,
          "toString",
          List.empty[Node],
          Type.getType(classOf[String])))
    }.getMessage
    assert(ex.contains("Failed to find field toString"))
  }

  test("unsupported function call") {
    // Function call unsupported if it is neither a static method, nor a getter method
    val ex = intercept[ClosureTranslationException] {
      expression[A](
        FunctionCall(
          arg[A],
          "java.lang.Object",
          "equals",
          List(Constant("")),
          BOOLEAN_TYPE
        )
      )
    }.getMessage
    assert(ex.contains("Unsupported function call java.lang.Object/equals"))
  }

  test("resolve the field access against schema") {
    // top level object cannot be null
    assertEqual(
      expression[A](Arithmetic("!=", arg[A], Constant(null), BOOLEAN_TYPE)),
      Literal(true))

    // top level object cannot be null
    assertEqual(
      expression[A](Arithmetic("==", arg[A], Constant(null), BOOLEAN_TYPE)),
      Literal(false))

    // Resolve an atomic type field
    assertEqual(
      expression[A](
        FunctionCall(
          arg[A],
          classOf[A].getName,
          "a2",
          List.empty[Node],
          Type.getType(classOf[String]))),
      UnresolvedAttribute("a2"))

    // Resolve a struct type field, no exception should be thrown
    val resolveStructField =
      generate[B](
        FunctionCall(
          arg[B],
          classOf[B].getName,
          "b1",
          List.empty[Node],
          Type.getType(classOf[A])),
        schemaFor[B])

    // cannot find a field name
    val ex = intercept[ClosureTranslationException](
      expression[A](
        FunctionCall(
          arg[A],
          classOf[A].getName,
          "toString",
          List.empty[Node],
          Type.getType(classOf[String])))).getMessage
    assert(ex.contains("Failed to find field"))

    // field type mismatch
    val ex2 = intercept[ClosureTranslationException](
      expression[A](
        FunctionCall(
          arg[A],
          classOf[A].getName,
          "a1",
          List.empty[Node],
          Type.getType(classOf[String])))).getMessage
    assert(ex2.contains("Failed to resolve field 'a1 because required data type StringType " +
      "mismatch with actual type IntegerType"))

  }

  test("flatten sub-fields of returned class") {

    // Flatten top level class A
    assertEqual(
      generate[A](arg[A], schemaFor[A]).toList,
      List(UnresolvedAttribute("a1"), UnresolvedAttribute("a2")))

    // Flatten top level class B
    val x = generate[B](arg[B], schemaFor[B]).toList
    assertEqual(
      generate[B](arg[B], schemaFor[B]).toList,
      List(UnresolvedAttribute("b1"), UnresolvedAttribute("b2")))

    // Flatten nested class A, add AssetNotNull when flatten
    val flattenNestedFields =
      generate[B](
        FunctionCall(
          arg[B],
          classOf[B].getName,
          "b1",
          List.empty[Node],
          Type.getType(classOf[A])),
        schemaFor[B]).toList

    assert(flattenNestedFields.length == 2)
    flattenNestedFields match {
      case GetStructField(AssertNotNull(UnresolvedAttribute("b1" :: Nil), _), _, Some("a1")) ::
        GetStructField(_, _, Some("a2")) :: Nil => // pass
      case _ => fail
    }
  }

  test("add NPE check for field access") {
    // Project a nested primitive field, add NPE check
    val nestedPrimitive =
      expression[B](
        FunctionCall(
          FunctionCall(
            arg[B],
            classOf[B].getName,
            "b1",
            List.empty[Node],
            Type.getType(classOf[A])),
          classOf[A].getName,
          "a2",
          List.empty[Node],
          Type.getType(classOf[String])))

    assertEqual(
      nestedPrimitive,
      NPEOnNull(
        UnresolvedAttribute("b1"),
        UnresolvedAttribute("b1.a2")
      ))
  }

  test("Float/Double NaN") {
    // TODO: Catalyst Float/Double NaN is not handled same as Java NaN.
  }

  private def assertEqual(a: Any, b: Any): Unit = {
    assert(a == b)
  }

  private def generate[T: ClassTag](
      node: Node,
      schema: StructType)
    : Seq[Expression] = {
    val generator = new ExpressionGenerator
    val loader = Thread.currentThread.getContextClassLoader
    val exprs = generator.generate(node, classTag[T].runtimeClass, schema)
    exprs
  }

  private def expression[T: ClassTag: TypeTag](node: Node): Expression = {
    val exprs = generate(node, schemaFor)
    assert(exprs.length == 1)
    exprs(0)
  }

  private def schemaFor[T: ClassTag: TypeTag]: StructType = {
    val dataType = Type.getType(classTag[T].runtimeClass).sqlType
    dataType match {
      case atomic: AtomicType => StructType(Seq(StructField("value", atomic)))
      case _ => ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    }
  }

  private def literal(v: Any): Literal = v match {
    case s: Char => Literal(s.toInt)
    case _ => Literal(v)
  }
}

object ExpressionGeneratorSuite {
  case class A(a1: Int, a2: String)
  case class B(b1: A, b2: A)
  case class C(c1: B, c2: Short)
}
