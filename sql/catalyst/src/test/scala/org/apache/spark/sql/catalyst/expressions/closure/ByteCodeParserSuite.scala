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
import org.apache.spark.sql.catalyst.expressions.closure.ByteCodeParser.{opString, UnsupportedOpcodeException}
import org.apache.spark.sql.catalyst.expressions.closure.ByteCodeParserSuite.{A, CreateClosureWithStackClassLoader}
import org.apache.spark.sql.catalyst.expressions.closure.DSL._

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

    assertEqual(
      parse(closure),
      ifElse(
        compareEqual(
          FunctionCall(
            obj = Constant("addOne"),
            className = "java.lang.Object",
            method = "equals",
            arguments = List(Constant(null)),
            dataType = BOOLEAN_TYPE),
          Constant(0)),
        Constant(32958.0D),
        Constant(32957.0D)))

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
      case FunctionCall(StaticField(_, "MODULE$", _), className, "ZERO", _, _) =>
        assert(className == ByteCodeParserSuite.getClass.getName)
      case _ => fail
    }
  }

  test("cast instructions") {
    val castChain = (v: Int) => {
      v.toLong.toFloat.toDouble.toInt.toDouble.toFloat.toLong.toInt.toFloat.toInt.toDouble.toLong
        .toDouble
    }

    assertEqual(
      parse(castChain),
      cast[Double](
        cast[Long](
          cast[Double](
            cast[Int](
              cast[Float](
                cast[Int](
                  cast[Long](
                    cast[Float](
                      cast[Double](
                        cast[Int](
                          cast[Double](
                            cast[Float](
                              cast[Long](
                                arg[Int]))))))))))))))

    // The result of I2S is an int, not a short.
    val castShort = (v: Int) => v.toShort + v
    assertEqual(
      parse(castShort),
      plus(
        cast[Int](
          cast[Short](arg[Int])),
        arg[Int]))

    // The result of I2B is an int, not a byte.
    val castByte = (v: Int) => v.toByte + v
    assertEqual(
      parse(castByte),
      plus(
        cast[Int](
          cast[Byte](arg[Int])),
        arg[Int]))

    // The result of I2C is an int, not a char.
    val castChar = (v: Int) => v.toChar + v
    assertEqual(
      parse(castChar),
      plus(
        cast[Int](
          cast[Char](arg[Int])),
        arg[Int]))

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
    assertEqual(parse(castIfToBoolean), gt(arg[Int], Constant(0)))

    // Eliminates the outer duplicate cast Cast[Int](Cast[Int](node))
    val castTwice = (v: Long) => v.toInt.toInt
    assertEqual(parse(castTwice), cast[Int](arg[Long]))

    // Eliminates the outer unnecessary casts Cast[Byte](Cast[Int](Cast[Byte](node)))
    val castByteIntByte = (v: Int) => v.toByte.toInt.toByte
    assertEqual(parse(castByteIntByte), cast[Byte](arg[Int]))

    val castShortIntShort = (v: Int) => v.toShort.toInt.toShort
    assertEqual(parse(castShortIntShort), cast[Short](arg[Int]))

    val castCharIntChar = (v: Int) => v.toChar.toInt.toChar
    assertEqual(parse(castCharIntChar), cast[Char](arg[Int]))

    val castIntLongInt = (v: Long) => v.toInt.toLong.toInt
    assertEqual(parse(castIntLongInt), cast[Int](arg[Long]))

    val castFloatDoubleFloat = (v: Double) => v.toFloat.toDouble.toFloat
    assertEqual(parse(castFloatDoubleFloat), cast[Float](arg[Double]))

    // Tests CHECKCAST
    val checkCast = (v: AnyRef) => v.asInstanceOf[Integer]
    assertEqual(parse(checkCast), cast[java.lang.Integer](arg[java.lang.Object]))
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
    assert(parse(returnVoid) == VOID)
    assert(parse(returnUnit) == StaticField("scala.Unit$", "MODULE$", getType(scala.Unit.getClass)))
  }

  test("arithmetic operation instructions, +, -, *, /, %, NEG, INC") {
    val intOperation = (v: Int) => (((-v + 3) - 4) * 7 / 2) % 5
    assertEqual(
      parse(intOperation),
      rem(
        div(
          mul(
            minus(
              plus(
                minus(Constant(0), arg[Int]),
                Constant(3)),
              Constant(4)),
            Constant(7)),
          Constant(2)),
        Constant(5)))

    val longOperation = (v: Long) => (((-v + 3L) - 4L) * 7L / 2L) % 5L
    assertEqual(
      parse(longOperation),
      rem(
        div(
          mul(
            minus(
              plus(
                minus(Constant(0L), arg[Long]),
                Constant(3L)),
              Constant(4L)),
            Constant(7L)),
          Constant(2L)),
        Constant(5L)))


    val floatOperation = (v: Float) => (((-v + 3F) - 4F) * 7F / 2F) % 5F
    assertEqual(
      parse(floatOperation),
      rem(
        div(
          mul(
            minus(
              plus(
                minus(Constant(0.0F), arg[Float]),
                Constant(3.0F)),
              Constant(4.0f)),
            Constant(7.0F)),
          Constant(2.0F)),
      Constant(5.0F)))

    val doubleOperation = (v: Double) => (((-v + 3D) - 4D) * 7D / 2D) % 5D
    assertEqual(
      parse(doubleOperation),
      rem(
        div(
          mul(
            minus(
              plus(
                minus(Constant(0.0D), arg[Double]),
                Constant(3.0D)),
              Constant(4.0D)),
            Constant(7.0D)),
          Constant(2.0D)),
        Constant(5.0D)))

    logTrace(parse(new IincTestClosure, classOf[Int]).treeString)
  }

  test("bitwise operation instructions, &, |, ^") {
    val intOperation = (v: Int) => ((v & 3) | 4) ^ 5
    val longOperation = (v: Long) => ((v & 3L) | 4L) ^ 5L
    assertEqual(
      parse(intOperation),
      bitwiseXor(
        bitwiseOr(
          bitwiseAnd(arg[Int], Constant(3)),
          Constant(4)),
        Constant(5)))

    assertEqual(
      parse(longOperation),
      bitwiseXor(
        bitwiseOr(
          bitwiseAnd(arg[Long], Constant(3L)),
          Constant(4L)),
        Constant(5L)))
  }

  test("compare and jump instructions") {
    // int compare, ==
    val intEqual = (v: Int) => v == 0 // IF_ICMPNE
    assertEqual(parse(intEqual), compareEqual(arg[Int], Constant(0)))

    // int compare, !=
    val intNotEqual = (v: Int) => v != 0 // // IF_ICMPEQ
    assertEqual(parse(intNotEqual), compareNotEqual(arg[Int], Constant(0)))

    // int compare, <=
    val intLessThanOrEqualsTo = (v: Int) => v <= 3  // IF_ICMPGT
    assertEqual(parse(intLessThanOrEqualsTo), le(arg[Int], Constant(3)))


    // int compare, >=
    val intGreaterThanOrEqualsTo = (v: Int) => v >= 3  // IF_ICMPLT
    assertEqual(parse(intGreaterThanOrEqualsTo), ge(arg[Int], Constant(3)))

    // int compare, <
    val intLessThan = (v: Int) => v < 3  // IF_ICMPGE
    assertEqual(parse(intLessThan), lt(arg[Int], Constant(3)))

    // int compare, >
    val intGreaterThan = (v: Int) => v > 3  // IF_ICMPLE
    assertEqual(parse(intGreaterThan), gt(arg[Int], Constant(3)))

    // reference null check
    val objectEqualsNull = (v: java.lang.Integer) => v.eq(null) // IFNONNULL
    assertEqual(
      parse(objectEqualsNull),
      compareEqual(arg[java.lang.Integer], Constant(null)))

    // reference equality check
    val objectEquals = (v: java.lang.Integer) => v.eq(v) // IF_ACMPNE
    assertEqual(
      parse(objectEquals),
      compareEqual(arg[java.lang.Integer], arg[java.lang.Integer]))

    // long compare, >
    val longCompareGreaterThan = (v: Long) => v > 3L  // LCMP, IFLE
    assertEqual(parse(longCompareGreaterThan), gt(arg[Long], Constant(3L)))

    // long compare, <
    val longCompareLessThan = (v: Long) => v < 3L  // LCMP, IFGE
    assertEqual(parse(longCompareLessThan), lt(arg[Long], Constant(3L)))

    // long compare, <=
    val longCompareLessThanOrEqualsTo = (v: Long) => v <= 3L  // LCMP, IFGT
    assertEqual(parse(longCompareLessThanOrEqualsTo), le(arg[Long], Constant(3L)))

    // long compare, >=
    val longCompareGreaterThanOrEqualsTo = (v: Long) => v >= 3L  // LCMP, IFLT
    assertEqual(parse(longCompareGreaterThanOrEqualsTo), ge(arg[Long], Constant(3L)))

    // long compare, ==
    val longCompareEquals = (v: Long) => v == 3L  // LCMP, IFNE
    assertEqual(parse(longCompareEquals), compareEqual(arg[Long], Constant(3L)))

    // long compare, !=
    val longCompareNotEquals = (v: Long) => v != 3L  // LCMP, IFEQ
    assertEqual(parse(longCompareNotEquals), compareNotEqual(arg[Long], Constant(3L)))

    // float compare, >
    val floatCompareGreaterThan = (v: Float) => v > 3.0F  // FCMPL, IFLE
    assertEqual(parse(floatCompareGreaterThan), gt(arg[Float], Constant(3.0F)))

    // float compare, <
    val floatCompareLessThan = (v: Float) => v < 3.0F  // FCMPL, IFGE
    assertEqual(parse(floatCompareLessThan), lt(arg[Float], Constant(3.0F)))

    // float compare, <=
    val floatCompareLessThanOrEqualsTo = (v: Float) => v <= 3.0F  // FCMPG, IFGT
    assertEqual(parse(floatCompareLessThanOrEqualsTo), le(arg[Float], Constant(3.0F)))

    // float compare, >=
    val floatCompareGreaterThanOrEqualsTo = (v: Float) => v >= 3.0F  // FCMPL, IFLT
    assertEqual(parse(floatCompareGreaterThanOrEqualsTo), ge(arg[Float], Constant(3.0F)))

    // float compare, ==
    val floatCompareEquals = (v: Float) => v == 3.0F  // FCMPL
    assertEqual(parse(floatCompareEquals), compareEqual(arg[Float], Constant(3.0F)))

    // float compare, !=
    val floatCompareNotEquals = (v: Float) => v != 3.0F  // FCMPL
    assertEqual(parse(floatCompareNotEquals), compareNotEqual(arg[Float], Constant(3.0F)))

    // double compare, >
    val doubleCompareGreaterThan = (v: Double) => v > 3.0D  // DCMPL, IFLE
    assertEqual(parse(doubleCompareGreaterThan), gt(arg[Double], Constant(3.0D)))

    // double compare, <
    val doubleCompareLessThan = (v: Double) => v < 3.0D  // DCMPG, IFGE
    assertEqual(parse(doubleCompareLessThan), lt(arg[Double], Constant(3.0D)))

    // double compare, <=
    val doubleCompareLessThanOrEqualsTo = (v: Double) => v <= 3.0D  // DCMPG, IFGT
    assertEqual(parse(doubleCompareLessThanOrEqualsTo), le(arg[Double], Constant(3.0D)))

    // double compare, >=
    val doubleCompareGreaterThanOrEqualsTo = (v: Double) => v >= 3.0D  // DCMPL, IFLT
    assertEqual(parse(doubleCompareGreaterThanOrEqualsTo), ge(arg[Double], Constant(3.0D)))

    // double compare, ==
    val doubleCompareEquals = (v: Double) => v == 3.0D  // DCMPL, IFNE
    assertEqual(parse(doubleCompareEquals), compareEqual(arg[Double], Constant(3.0D)))

    // double compare, !=
    val doubleCompareNotEquals = (v: Double) => v != 3.0D  // DCMPL, IFEQ
    assertEqual(parse(doubleCompareNotEquals), compareNotEqual(arg[Double], Constant(3.0D)))

    val booleanCompare = (v: Int) => {
      val flag = v > 0
      if (flag) v else 0
    }

    assertEqual(
      parse(booleanCompare),
      ifElse(
        le((arg[Int]), Constant(0)),
        Constant(0),
        arg[Int]))

    // TODO: Add test cases for Float NaN, Infinity
  }

  // JVM treats boolean/byte/short/char as int internally. so we should do proper casting
  // on the input argument or the return value.
  test("cast input argument type and return type for boolean/byte/short/char") {
    // Boolean Argument is casted to Int type.
    val castBoolean = (v: Boolean) => if (v) 1 else 0
    assertEqual(
      parse(castBoolean),
      ifElse(
        compareEqual(cast[Int](arg[Boolean]), Constant(0)),
        Constant(0),
        Constant(1)))

    val castByte = (v: Byte) => v.toLong
    assertEqual(
      parse(castByte),
      cast[Long](
        cast[Int](
          arg[Byte])))

    val castShort = (v: Short) => v.toLong
    assertEqual(
      parse(castShort),
      cast[Long](
        cast[Int](
          arg[Short])))

    val castChar = (v: Char) => v.toLong
    assertEqual(
      parse(castChar),
      cast[Long](
        cast[Int](
          arg[Char])))

    // Optimizes Cast[Boolean](Cast[Int](node)) to node if node is Boolean type
    val castBoolean2 = (v: Boolean) => v
    assertEqual(parse(castBoolean2), arg[Boolean])
    val castByte2 = (v: Byte) => v
    assertEqual(parse(castByte2), arg[Byte])
    val castShort2 = (v: Short) => v
    assertEqual(parse(castShort2), arg[Short])
    val castChar2 = (v: Char) => v
    assertEqual(parse(castChar2), arg[Char])
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

  private def parse[T: ClassTag, R](function: (T) => R): Node = {
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
        s"stack operation ${pops.flatMap(opString(_)).mkString(", ")} result $nodeTree doesn't " +
          s"match real function call result ${obj.call(0)}")
      nodeTree
    } finally {
      Thread.currentThread().setContextClassLoader(oldLoader)
    }
  }

  private def assertEqual(a: Any, b: Any) = assert(a == b)
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
