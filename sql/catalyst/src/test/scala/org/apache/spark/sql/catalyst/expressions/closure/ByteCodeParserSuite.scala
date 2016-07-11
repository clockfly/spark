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

import java.io.{ByteArrayInputStream, InputStream}

import scala.collection.JavaConverters._
import scala.collection.immutable.{::}
import scala.reflect.ClassTag
import scala.reflect.classTag

import org.apache.xbean.asm5.{ClassReader, ClassWriter}
import org.apache.xbean.asm5.Opcodes
import org.apache.xbean.asm5.Opcodes.IRETURN
import org.apache.xbean.asm5.Type
import org.apache.xbean.asm5.Type._
import org.apache.xbean.asm5.tree.{ClassNode, InsnNode, LdcInsnNode, MethodNode}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.closure.ByteCodeParser.{opString, Argument, Cast, Constant, FunctionCall, Node, Static, This, UnsupportedOpcodeException}
import org.apache.spark.sql.catalyst.expressions.closure.ByteCodeParserSuite.{A, CreateClosureWithStackClassLoader}

class ByteCodeParserSuite extends SparkFunSuite {

  // Test constant instructions (xCONST_n, LDC x, BIPUSH, SIPUSH) and variable load and store
  // instructions (xSTORE, xLOAD)
  test("constant value instructions and local variable instructions") {
    val closure = (v: Int) => {
      val nullObject = null // ACONST_null
      val s = "addOne" // LDC String, ASTORE
      val arg =
        if (s != nullObject) { // ALOAD
          1
        } else {
          0
        }

      val im1 = -1 // ICONST_M1, ISTORE
      val i0 = 0 // ICONST_0
      val i1 = 1 // ICONST_1
      val i2 = 2 // ICONST_2
      val i3 = 3 // ICONST_3
      val i4 = 4 // ICONST_4
      val i5 = 5 // ICONST_5
      val f0 = 0f // FCONST_0, FSTORE
      val f1 = 1f // FCONST_1
      val f2 = 2f // FCONST_2
      val l0 = 0L // LCONST_0, LSTORE
      val l1 = 1L // LCONST_1
      val d0 = 0D // DCONST_0, DSTORE
      val d1 = 1D // DCONST_1

      val bipush = 30 // BIPUSH
      val sipush = Byte.MaxValue + 1 // SIPUSH
      val ldci = Short.MaxValue + 1 // LDC int, ISTORE
      val ldcf = 3.0f // LDC float, FSTORE
      val ldcl = 4L // LDC long, LSTORE
      val ldcd = 5.0d // LDC double, DSTORE

      val sum1 = arg + im1 + i0 + i1 + i2 + i3 + i4 + i5 + bipush + sipush + ldci // ILOAD
      val sum2 = sum1 + f0 + f1 + f2 + ldcf // FLOAD
      val sum3 = sum2 + l0 + l1 + ldcl // LLOAD
      sum3 + d0 + d1 + ldcd // DLOAD
    }

    assert(parse(closure).treeString ==
      """If[D]((java.lang.Object.equals(addOne, null)) == 0)
        |  Constant[D](32958.0)
        |  Constant[D](32957.0)
        |""".stripMargin)

    // "LDC x" is unsupported if x is a class reference...
    val ldc_reference = (v: Int) => {
      classOf[java.lang.Integer]  // LDC class java/lang/Integer
    }
    val ex = intercept[UnsupportedOpcodeException] {
      parse(ldc_reference)
    }.getMessage
    assert(ex.contains("Unsupported opcode LDC"))
  }

  test("field instructions GETSTATIC") {
    val getStatic = (v: Int) => ByteCodeParserSuite.ZERO // GETSTATIC
    val parser = new ByteCodeParser
    val staticNode = parser.parse(getStatic.getClass, classOf[Int])
    staticNode match {
      case FunctionCall(Static(_, "MODULE$", _), className, "ZERO", _, _) =>
        assert(className == ByteCodeParserSuite.getClass.getName)
      case _ => fail
    }
  }

  test("cast instructions") {
    val castChain = (v: Int) => {
      v.toLong.toFloat.toDouble.toInt.toDouble.toFloat.toLong.toInt.toFloat.toInt.toDouble.toLong
        .toDouble
    }
    assert(parse(castChain).treeString ==
      s"""Cast[D]
         |  Cast[J]
         |    Cast[D]
         |      Cast[I]
         |        Cast[F]
         |          Cast[I]
         |            Cast[J]
         |              Cast[F]
         |                Cast[D]
         |                  Cast[I]
         |                    Cast[D]
         |                      Cast[F]
         |                        Cast[J]
         |                          Argument[I]
         |""".stripMargin)

    // The result of I2S is an int, not a short.
    val castShort = (v: Int) => v.toShort + v
    assert(parse(castShort).treeString ==
      s"""Arithmetic[I](+)
         |  Cast[I]
         |    Cast[S]
         |      Argument[I]
         |  Argument[I]
         |""".stripMargin)

    // The result of I2B is an int, not a byte.
    val castByte = (v: Int) => v.toByte + v
    assert(parse(castByte).treeString ==
      s"""Arithmetic[I](+)
          |  Cast[I]
          |    Cast[B]
          |      Argument[I]
          |  Argument[I]
          |""".stripMargin)

    // The result of I2C is an int, not a char.
    val castChar = (v: Int) => v.toChar + v
    assert(parse(castChar).treeString ==
      s"""Arithmetic[I](+)
          |  Cast[I]
          |    Cast[C]
          |      Argument[I]
          |  Argument[I]
          |""".stripMargin)


    // Tests constant folding.
    val constantFolding = (v: Int) => {
      // byte is sign-extended to an int
      val byte = (Byte.MaxValue + 2).toByte
      // short is sign-extended to an int
      val short = (Short.MaxValue + 2).toShort
      // short is zero-extended to an int
      val char = (Char.MaxValue.toInt + 2).toChar

      val float = 3.toFloat + 4L.toFloat + 5.5D.toFloat
      val int = 6.2f.toInt + 7L.toInt + 8.8D.toInt
      val long = 9.toLong + 10.4f.toLong + 11.8D.toLong
      val double = 12.toDouble + 12.7f.toDouble + 13L.toDouble

      double + long + int + float + char + short + byte
    }
    assert(parse(constantFolding) == Constant(constantFolding(0)))

    // Turns Cast[Boolean](If(condition, left, right)) to
    // If(condition, Cast[Boolean](left), Cast[Boolean](right))
    val castIfToBoolean = (v: Int) => {
      v > 0
    }

    assert(parse(castIfToBoolean).treeString ==
      s"""Arithmetic[Z](>)
         |  Argument[I]
         |  Constant[I](0)
         |""".stripMargin)

    // Eliminates the outer duplicate cast Cast[Int](Cast[Int](node))
    val castTwice = (v: Long) => v.toInt.toInt
    assert(parse(castTwice).treeString ==
      s"""Cast[I]
         |  Argument[J]
         |""".stripMargin)

    // Eliminates the outer unnecessary casts Cast[Byte](Cast[Int](Cast[Byte](node)))
    val castByteIntByte = (v: Int) => v.toByte.toInt.toByte
    assert(parse(castByteIntByte).treeString ==
      s"""Cast[B]
          |  Argument[I]
          |""".stripMargin)
    val castShortIntShort = (v: Int) => v.toShort.toInt.toShort
    assert(parse(castShortIntShort).treeString ==
      s"""Cast[S]
          |  Argument[I]
          |""".stripMargin)
    val castCharIntChar = (v: Int) => v.toChar.toInt.toChar
    assert(parse(castCharIntChar).treeString ==
      s"""Cast[C]
          |  Argument[I]
          |""".stripMargin)
    val castIntLongInt = (v: Long) => v.toInt.toLong.toInt
    assert(parse(castIntLongInt).treeString ==
      s"""Cast[I]
          |  Argument[J]
          |""".stripMargin)
    val castFloatDoubleFloat = (v: Double) => v.toFloat.toDouble.toFloat
    assert(parse(castFloatDoubleFloat).treeString ==
      s"""Cast[F]
          |  Argument[D]
          |""".stripMargin)

    // Tests CHECKCAST
    val checkCast = (v: AnyRef) => v.asInstanceOf[Integer]
    assert(parse(checkCast).treeString ==
      s"""Cast[Ljava/lang/Integer;]
         |  Argument[Ljava/lang/Object;]
         |""".stripMargin)
  }

  test("function call instructions") {
    val func = new Function1[A, Double] {
      private def privateMethod(a: Double): Double = a
      def publicMethod(a: Double): Double = a
      def apply(v: A): Double = {
          Math.sqrt(publicMethod(privateMethod(v.get)))
      }
    }
    assert(parse(func) match {
      case FunctionCall(
        Constant(null),
        "java.lang.Math",
        "sqrt",
          FunctionCall(
            This(_),
            _,
            "publicMethod",
            FunctionCall(
              This(_),
              _,
              "privateMethod",
              FunctionCall(
                Argument(_),
                _,
                "get",
                Nil,
                _):: Nil,
              _) :: Nil,
            _) :: Nil,
          Type.DOUBLE_TYPE) => true
      case _ => false
    })
  }

  test("return instructions") {
    val returnDouble = (v: Double) => v
    val returnLong = (v: Long) => v
    val returnFloat = (v: Float) => v
    val returnInt = (v: Int) => v
    val returnByte = (v: Int) => v.toByte
    val returnShort = (v: Int) => v.toShort
    val returnChar = (v: Int) => v.toChar
    val returnObject = (v: AnyRef) => v
    val returnVoid = (v: Int) => {}
    val returnUnit = (v: Int) => Unit

    assert(parse(returnDouble) == Argument(Type.DOUBLE_TYPE))
    assert(parse(returnLong) == Argument(Type.LONG_TYPE))
    assert(parse(returnFloat) == Argument(Type.FLOAT_TYPE))
    assert(parse(returnInt) == Argument(Type.INT_TYPE))
    assert(parse(returnByte) == Cast(Argument(Type.INT_TYPE), Type.BYTE_TYPE))
    assert(parse(returnShort) == Cast(Argument(Type.INT_TYPE), Type.SHORT_TYPE))
    assert(parse(returnChar) == Cast(Argument(Type.INT_TYPE), Type.CHAR_TYPE))
    assert(parse(returnObject) == Argument(Type.getType(classOf[java.lang.Object])))
    assert(parse(returnVoid) == ByteCodeParser.VOID)
    assert(parse(returnUnit) == Static("scala.Unit$", "MODULE$", getType(scala.Unit.getClass)))
  }

  test("arithmetic operation instructions, +, -, *, /, %, NEG, INC") {
    val intOperation = (v: Int) => (((-v + 3) - 4) * 7 / 2) % 5
    assert(parse(intOperation).treeString ==
      s"""Arithmetic[I](%)
         |  Arithmetic[I](/)
         |    Arithmetic[I](*)
         |      Arithmetic[I](-)
         |        Arithmetic[I](+)
         |          Arithmetic[I](-)
         |            Constant[I](0)
         |            Argument[I]
         |          Constant[I](3)
         |        Constant[I](4)
         |      Constant[I](7)
         |    Constant[I](2)
         |  Constant[I](5)
         |""".stripMargin)

    val longOperation = (v: Long) => (((-v + 3L) - 4L) * 7L / 2L) % 5L
    assert(parse(longOperation).treeString ==
      s"""Arithmetic[J](%)
          |  Arithmetic[J](/)
          |    Arithmetic[J](*)
          |      Arithmetic[J](-)
          |        Arithmetic[J](+)
          |          Arithmetic[J](-)
          |            Constant[J](0)
          |            Argument[J]
          |          Constant[J](3)
          |        Constant[J](4)
          |      Constant[J](7)
          |    Constant[J](2)
          |  Constant[J](5)
          |""".stripMargin)

    val floatOperation = (v: Float) => (((-v + 3F) - 4F) * 7F / 2F) % 5F
    assert(parse(floatOperation).treeString ==
      s"""Arithmetic[F](%)
          |  Arithmetic[F](/)
          |    Arithmetic[F](*)
          |      Arithmetic[F](-)
          |        Arithmetic[F](+)
          |          Arithmetic[F](-)
          |            Constant[F](0.0)
          |            Argument[F]
          |          Constant[F](3.0)
          |        Constant[F](4.0)
          |      Constant[F](7.0)
          |    Constant[F](2.0)
          |  Constant[F](5.0)
          |""".stripMargin)

    val doubleOperation = (v: Double) => (((-v + 3D) - 4D) * 7D / 2D) % 5D
    assert(parse(doubleOperation).treeString ==
      s"""Arithmetic[D](%)
          |  Arithmetic[D](/)
          |    Arithmetic[D](*)
          |      Arithmetic[D](-)
          |        Arithmetic[D](+)
          |          Arithmetic[D](-)
          |            Constant[D](0.0)
          |            Argument[D]
          |          Constant[D](3.0)
          |        Constant[D](4.0)
          |      Constant[D](7.0)
          |    Constant[D](2.0)
          |  Constant[D](5.0)
          |""".stripMargin)

    logTrace(parse(new IincTestClosure, classOf[Int]).treeString)
  }

  test("bitwise operation instructions, &, |, ^") {
    val intOperation = (v: Int) => ((v & 3) | 4) ^ 5
    val longOperation = (v: Long) => ((v & 3L) | 4L) ^ 5L
    assert(parse(intOperation).toString == "((Argument & 3) | 4) ^ 5")
    assert(parse(longOperation).toString == "((Argument & 3) | 4) ^ 5")
  }

  test("compare and jump instructions") {
    // int compare, ==
    val intEqual = (v: Int) => v == 0 // IF_ICMPNE
    assert(parse(intEqual).treeString ==
      s"""Arithmetic[Z](==)
         |  Argument[I]
         |  Constant[I](0)
         |""".stripMargin)

    // int compare, !=
    val intNotEqual = (v: Int) => v != 0 // // IF_ICMPEQ
    assert(parse(intNotEqual).treeString ==
      s"""Arithmetic[Z](!=)
          |  Argument[I]
          |  Constant[I](0)
          |""".stripMargin)

    // int compare, <=
    val intLessThanOrEqualsTo = (v: Int) => v <= 3  // IF_ICMPGT
    assert(parse(intLessThanOrEqualsTo).treeString ==
      s"""Arithmetic[Z](<=)
         |  Argument[I]
         |  Constant[I](3)
         |""".stripMargin)

    // int compare, >=
    val intGreaterThanOrEqualsTo = (v: Int) => v >= 3  // IF_ICMPLT
    assert(parse(intGreaterThanOrEqualsTo).treeString ==
      s"""Arithmetic[Z](>=)
          |  Argument[I]
          |  Constant[I](3)
          |""".stripMargin)

    // int compare, <
    val intLessThan = (v: Int) => v < 3  // IF_ICMPGE
    assert(parse(intLessThan).treeString ==
      s"""Arithmetic[Z](<)
          |  Argument[I]
          |  Constant[I](3)
          |""".stripMargin)

    // int compare, >
    val intGreaterThan = (v: Int) => v > 3  // IF_ICMPLE
    assert(parse(intGreaterThan).treeString ==
      s"""Arithmetic[Z](>)
          |  Argument[I]
          |  Constant[I](3)
          |""".stripMargin)

    // reference null check
    val objectEqualsNull = (v: java.lang.Integer) => v.eq(null) // IFNONNULL
    assert(parse(objectEqualsNull).toString == "Argument == null")

    // reference equality check
    val objectEquals = (v: java.lang.Integer) => v.eq(v) // IF_ACMPNE
    assert(parse(objectEquals).toString == "Argument == Argument")

    // long compare, >
    val longCompareGreaterThan = (v: Long) => v > 3L  // LCMP, IFLE
    assert(parse(longCompareGreaterThan).toString == "Argument > 3")

    // long compare, <
    val longCompareLessThan = (v: Long) => v < 3L  // LCMP, IFGE
    assert(parse(longCompareLessThan).toString == "Argument < 3")

    // long compare, <=
    val longCompareLessThanOrEqualsTo = (v: Long) => v <= 3L  // LCMP, IFGT
    assert(parse(longCompareLessThanOrEqualsTo).toString == "Argument <= 3")

    // long compare, >=
    val longCompareGreaterThanOrEqualsTo = (v: Long) => v >= 3L  // LCMP, IFLT
    assert(parse(longCompareGreaterThanOrEqualsTo).toString == "Argument >= 3")

    // long compare, ==
    val longCompareEquals = (v: Long) => v == 3L  // LCMP, IFNE
    assert(parse(longCompareEquals).toString == "Argument == 3")

    // long compare, !=
    val longCompareNotEquals = (v: Long) => v != 3L  // LCMP, IFEQ
    assert(parse(longCompareNotEquals).toString == "Argument != 3")

    // float compare, >
    val floatCompareGreaterThan = (v: Float) => v > 3.0F  // FCMPL, IFLE
    assert(parse(floatCompareGreaterThan).toString == "Argument > 3.0")

    // float compare, <
    val floatCompareLessThan = (v: Float) => v < 3.0F  // FCMPL, IFGE
    assert(parse(floatCompareLessThan).toString == "Argument < 3.0")

    // float compare, <=
    val floatCompareLessThanOrEqualsTo = (v: Float) => v <= 3.0F  // FCMPG, IFGT
    assert(parse(floatCompareLessThanOrEqualsTo).toString == "Argument <= 3.0")

    // float compare, >=
    val floatCompareGreaterThanOrEqualsTo = (v: Float) => v >= 3.0F  // FCMPL, IFLT
    assert(parse(floatCompareGreaterThanOrEqualsTo).toString == "Argument >= 3.0")

    // float compare, ==
    val floatCompareEquals = (v: Float) => v == 3.0F  // FCMPL
    assert(parse(floatCompareEquals).toString == "Argument == 3.0")

    // float compare, !=
    val floatCompareNotEquals = (v: Float) => v != 3.0F  // FCMPL
    assert(parse(floatCompareNotEquals).toString == "Argument != 3.0")

    // double compare, >
    val doubleCompareGreaterThan = (v: Double) => v > 3.0D  // DCMPL, IFLE
    assert(parse(doubleCompareGreaterThan).toString == "Argument > 3.0")

    // double compare, <
    val doubleCompareLessThan = (v: Double) => v < 3.0D  // DCMPG, IFGE
    assert(parse(doubleCompareLessThan).toString == "Argument < 3.0")

    // double compare, <=
    val doubleCompareLessThanOrEqualsTo = (v: Double) => v <= 3.0D  // DCMPG, IFGT
    assert(parse(doubleCompareLessThanOrEqualsTo).toString == "Argument <= 3.0")

    // double compare, >=
    val doubleCompareGreaterThanOrEqualsTo = (v: Double) => v >= 3.0D  // DCMPL, IFLT
    assert(parse(doubleCompareGreaterThanOrEqualsTo).toString == "Argument >= 3.0")

    // double compare, ==
    val doubleCompareEquals = (v: Double) => v == 3.0D  // DCMPL, IFNE
    assert(parse(doubleCompareEquals).toString == "Argument == 3.0")

    // double compare, !=
    val doubleCompareNotEquals = (v: Double) => v != 3.0D  // DCMPL, IFEQ
    assert(parse(doubleCompareNotEquals).toString == "Argument != 3.0")

    val booleanCompare = (v: Int) => {
      val flag = v > 0
      if (flag) v else 0
    }

    assert(parse(booleanCompare).treeString ==
      s"""If[I](Argument <= 0)
         |  Constant[I](0)
         |  Argument[I]
         |""".stripMargin)

    // TODO: Add test cases for Float NaN, Infinity
  }

  // JVM treats boolean/byte/short/char as int internally. so we should do proper casting
  // on the input argument or the return value.
  test("cast input argument type and return type for boolean/byte/short/char") {
    // Boolean Argument is casted to Int type.
    val castBoolean = (v: Boolean) => if (v) 1 else 0
    assert(parse(castBoolean).treeString ==
      s"""If[I](Cast(Argument,I) == 0)
         |  Constant[I](0)
         |  Constant[I](1)
         |""".stripMargin)

    val castByte = (v: Byte) => v.toLong
    assert(parse(castByte).treeString ==
      s"""Cast[J]
         |  Cast[I]
         |    Argument[B]
         |""".stripMargin)

    val castShort = (v: Short) => v.toLong
    assert(parse(castShort).treeString ==
      s"""Cast[J]
          |  Cast[I]
          |    Argument[S]
          |""".stripMargin)

    val castChar = (v: Char) => v.toLong
    assert(parse(castChar).treeString ==
      s"""Cast[J]
          |  Cast[I]
          |    Argument[C]
          |""".stripMargin)

    // Optimizes Cast[Boolean](Cast[Int](node)) to node if node is Boolean type
    val castBoolean2 = (v: Boolean) => v
    val x = parse(castBoolean2).treeString
    assert(parse(castBoolean2).treeString == "Argument[Z]\n")
    val castByte2 = (v: Byte) => v
    assert(parse(castByte2).treeString == "Argument[B]\n")
    val castShort2 = (v: Short) => v
    assert(parse(castShort2).treeString == "Argument[S]\n")
    val castChar2 = (v: Char) => v
    assert(parse(castChar2).treeString == "Argument[C]\n")
  }

  import Opcodes._
  test("stack instructions like POP, PO2, DUP, DUP_X2...") {
    implicit val classLoader = new CreateClosureWithStackClassLoader(
      Thread.currentThread().getContextClassLoader)
    // POP form: 1::Nil, expected stack: 6
    assert(createAndParseClosure(List[Any](5, 6), POP::Nil) == Constant(6))
    // POP2 form: 1::1::Nil, expected stack: 7
    assert(createAndParseClosure(List[Any](5, 6, 7), POP2::Nil) == Constant(7))
    // POP2 form: 2::Nil, expected stack: 6
    assert(createAndParseClosure(List[Any](5L, 6), POP2::Nil) == Constant(6))
    // DUP form: 1::Nil, expected stack: 5, 5
    assert(createAndParseClosure(List[Any](5), DUP::Nil) == Constant(5))
    // DUP form: 1::1::Nil, expected stack: 5, 6, 5, 6
    assert(createAndParseClosure(List[Any](5, 6), DUP2::Nil) == Constant(5))
    assert(createAndParseClosure(List[Any](5, 6), DUP2::POP::Nil) == Constant(6))
    assert(createAndParseClosure(List[Any](5, 6), DUP2::POP2::Nil) == Constant(5))
    assert(createAndParseClosure(List[Any](5, 6), DUP2::POP2::POP::Nil) == Constant(6))
    // DUP2 form: 2::Nil, expected stack: 5L, 5L, 6
    assert(createAndParseClosure(List[Any](5L, 6), DUP2::POP2::POP2::Nil) == Constant(6))
    // DUP_X1 form: 1::1::Nil, expected stack: 5, 6, 5, 7
    assert(createAndParseClosure(List[Any](5, 6, 7), DUP_X1::Nil) == Constant(5))
    assert(createAndParseClosure(List[Any](5, 6, 7), DUP_X1::POP::Nil) == Constant(6))
    assert(createAndParseClosure(List[Any](5, 6, 7), DUP_X1::POP2::Nil) == Constant(5))
    assert(createAndParseClosure(List[Any](5, 6, 7), DUP_X1::POP2::POP::Nil) == Constant(7))
    // DUP_X2 form: 1::1::1::Nil, expected stack: 5, 6, 7, 5, 8
    assert(createAndParseClosure(List[Any](5, 6, 7, 8), DUP_X2::Nil) == Constant(5))
    assert(createAndParseClosure(List[Any](5, 6, 7, 8), DUP_X2::POP::Nil) == Constant(6))
    assert(createAndParseClosure(List[Any](5, 6, 7, 8), DUP_X2::POP2::Nil) == Constant(7))
    assert(createAndParseClosure(List[Any](5, 6, 7, 8), DUP_X2::POP2::POP::Nil) == Constant(5))
    assert(createAndParseClosure(List[Any](5, 6, 7, 8), DUP_X2::POP2::POP2::Nil) == Constant(8))
    // DUP_X2 form: 1::2::Nil, expected stack: 5, 6L, 5, 7
    assert(createAndParseClosure(List[Any](5, 6L, 7), DUP_X2::Nil) == Constant(5))
    assert(createAndParseClosure(List[Any](5, 6L, 7), DUP_X2::POP::POP2::Nil) == Constant(5))
    assert(createAndParseClosure(List[Any](5, 6L, 7), DUP_X2::POP::POP2::POP::Nil) == Constant(7))
    // DUP2_X1 form: 1::1::1::Nil, expected stack: 5, 6, 7, 5, 6
    assert(createAndParseClosure(List[Any](5, 6, 7), DUP2_X1::Nil) == Constant(5))
    assert(createAndParseClosure(List[Any](5, 6, 7), DUP2_X1::POP::Nil) == Constant(6))
    assert(createAndParseClosure(List[Any](5, 6, 7), DUP2_X1::POP2::Nil) == Constant(7))
    assert(createAndParseClosure(List[Any](5, 6, 7), DUP2_X1::POP2::POP::Nil) == Constant(5))
    assert(createAndParseClosure(List[Any](5, 6, 7), DUP2_X1::POP2::POP2::Nil) == Constant(6))
    // DUP2_X1 form: 2::1::Nil, expected stack: 5L, 6, 5L, 7
    assert(createAndParseClosure(List[Any](5L, 6, 7), DUP2_X1::POP2::Nil) == Constant(6))
    assert(
      createAndParseClosure(List[Any](5L, 6, 7), DUP2_X1::POP2::POP::POP2::Nil) == Constant(7))
    // DUP2_X2 form: 1::1::1::1::Nil, expected stack: 5, 6, 7, 8, 5, 6, 9
    assert(createAndParseClosure(List[Any](5, 6, 7, 8, 9), DUP2_X2::Nil) == Constant(5))
    assert(createAndParseClosure(List[Any](5, 6, 7, 8, 9), DUP2_X2::POP::Nil) == Constant(6))
    assert(createAndParseClosure(List[Any](5, 6, 7, 8, 9), DUP2_X2::POP2::Nil) == Constant(7))
    assert(createAndParseClosure(List[Any](5, 6, 7, 8, 9), DUP2_X2::POP2::POP::Nil) == Constant(8))
    assert(
      createAndParseClosure(List[Any](5, 6, 7, 8, 9), DUP2_X2::POP2::POP2::Nil) == Constant(5))
    assert(createAndParseClosure(
      List[Any](5, 6, 7, 8, 9), DUP2_X2::POP2::POP2::POP::Nil) == Constant(6))
    assert(createAndParseClosure(
      List[Any](5, 6, 7, 8, 9), DUP2_X2::POP2::POP2::POP2::Nil) == Constant(9))
    // DUP2_X2 form: 2::1::1::Nil, expected stack: 5L, 6, 7 ,5L, 8
    assert(createAndParseClosure(List[Any](5L, 6, 7, 8), DUP2_X2::POP2::Nil) == Constant(6))
    assert(createAndParseClosure(List[Any](5L, 6, 7, 8), DUP2_X2::POP2::POP::Nil) == Constant(7))
    assert(
      createAndParseClosure(List[Any](5L, 6, 7, 8), DUP2_X2::POP2::POP2::POP2::Nil) == Constant(8))
  }

  private def parse(closure: AnyRef, argumentType: Class[_]): Node = {
    val parser = new ByteCodeParser()
    parser.parse(closure.getClass, argumentType)
  }

  private def parse[T: ClassTag, R](function: Function1[T, R]): Node = {
    val parser = new ByteCodeParser()
    parser.parse(function.getClass, classTag[T].runtimeClass)
  }

  private def createAndParseClosure(
      stack: List[Any],
      pops: List[Int])(implicit loader: CreateClosureWithStackClassLoader): Node = {
    val oldLoader = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      val clazz = loader.createClosure(stack, pops.map(new InsnNode(_)))
      val obj = clazz.newInstance()
      val nodeTree = parse(obj, classOf[Int])
      assert(
        nodeTree == Constant(obj.call(0)),
        s"stack operation ${pops.flatMap(opString(_)).mkString(", ")} result ${nodeTree} doesn't " +
          s"match real function call result ${obj.call(0)}")
      nodeTree
    } finally {
      Thread.currentThread().setContextClassLoader(oldLoader)
    }
  }
}

object ByteCodeParserSuite {
  val ZERO = 0
  var testInt = 3
  trait A {
    def get: Double
  }
  case class AImpl(a: Double) extends A {
    override def get: Double = a
  }

  // Helper class to create a closure with required stack data and a sequence of stack operations.
  class CreateClosureWithStackClassLoader(parent: ClassLoader) extends ClassLoader(parent) {
    private var classes = Map.empty[String, Array[Byte]]

    override def getResourceAsStream(name: String): InputStream = {
      val resource = classes.get(name).map(new ByteArrayInputStream(_))
      resource.getOrElse(super.getResourceAsStream(name))
    }

    private val rand = new java.util.Random()
    private def randInt(): Int = (rand.nextDouble() * Int.MaxValue).toInt

    def createClosure(
        stack: List[Any],
        operations: List[InsnNode])
      : Class[Call] = {
      val templateClass = getResourceAsStream(classOf[CreateClosureWithStackTemplateClass]
        .getName.replace('.', '/') + ".class")
      val reader = new ClassReader(templateClass)
      val classNode = new ClassNode()
      reader.accept(classNode, 0)
      classNode.name = classNode.name + randInt()
      val methods = classNode.methods.asInstanceOf[java.util.List[MethodNode]]
      val method = methods.asScala.find(_.name == "call").get
      method.instructions.clear()
      stack.reverse.foreach { data =>
        method.instructions.add(new LdcInsnNode(data))
      }
      operations.foreach { instruction =>
        method.instructions.add(instruction)
      }
      method.instructions.add(new InsnNode(IRETURN))
      val writer = new ClassWriter(ClassWriter.COMPUTE_MAXS|ClassWriter.COMPUTE_FRAMES)
      classNode.accept(writer)
      val byteCode = writer.toByteArray
      val className = classNode.name.replaceAll("/", ".")
      classes += (classNode.name + ".class") -> byteCode
      defineClass(className, byteCode, 0, byteCode.length).asInstanceOf[Class[Call]]
    }
  }

  trait Call {
    def call(a: Int): Int
  }

  class CreateClosureWithStackTemplateClass extends Call {
    def call(a: Int): Int = a
  }
}
