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

import org.apache.xbean.asm5.Type
import org.apache.xbean.asm5.Type._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.closure.DSL._

class DSLSuite extends SparkFunSuite {
  import java.lang.Math.abs

  private val rand = new java.util.Random()
  val (i: Array[Int], f: Array[Float], d: Array[Double], l: Array[Long]) =
    (randomInts(2), randomFloats(2), randomDoubles(2), randomLongs(2))

  test("arithmetic and logical operators") {
    assert(plus(Constant(i(0)), Constant(i(1))) == Constant(i(0) + i(1)))
    assert(plus(Constant(f(0)), Constant(f(1))) == Constant(f(0) + f(1)))
    assert(plus(Constant(d(0)), Constant(d(1))) == Constant(d(0) + d(1)))
    assert(plus(Constant(l(0)), Constant(l(1))) == Constant(l(0) + l(1)))
    assert(plus(Constant(i(0)), arg[Int]) == Arithmetic("+", Constant(i(0)), arg[Int], INT_TYPE))

    assert(minus(Constant(i(0)), Constant(i(1))) == Constant(i(0) - i(1)))
    assert(minus(Constant(f(0)), Constant(f(1))) == Constant(f(0) - f(1)))
    assert(minus(Constant(d(0)), Constant(d(1))) == Constant(d(0) - d(1)))
    assert(minus(Constant(l(0)), Constant(l(1))) == Constant(l(0) - l(1)))
    assert(minus(Constant(i(0)), arg[Int]) == Arithmetic("-", Constant(i(0)), arg[Int], INT_TYPE))

    assert(mul(Constant(i(0)), Constant(i(1))) == Constant(i(0) * i(1)))
    assert(mul(Constant(f(0)), Constant(f(1))) == Constant(f(0) * f(1)))
    assert(mul(Constant(d(0)), Constant(d(1))) == Constant(d(0) * d(1)))
    assert(mul(Constant(l(0)), Constant(l(1))) == Constant(l(0) * l(1)))
    assert(mul(Constant(i(0)), arg[Int]) == Arithmetic("*", Constant(i(0)), arg[Int], INT_TYPE))

    assert(div(Constant(i(0)), Constant(abs(i(1) + 1))) == Constant(i(0) / abs(i(1) + 1)))
    assert(div(Constant(f(0)), Constant(abs(f(1) + 1))) == Constant(f(0) / abs(f(1) + 1)))
    assert(div(Constant(d(0)), Constant(abs(d(1) + 1))) == Constant(d(0) / abs(d(1) + 1)))
    assert(div(Constant(l(0)), Constant(abs(l(1) + 1))) == Constant(l(0) / abs(l(1) + 1)))
    assert(div(Constant(i(0)), arg[Int]) == Arithmetic("/", Constant(i(0)), arg[Int], INT_TYPE))

    assert(rem(Constant(i(0)), Constant(i(1))) == Constant(i(0) % i(1)))
    assert(rem(Constant(f(0)), Constant(f(1))) == Constant(f(0) % f(1)))
    assert(rem(Constant(d(0)), Constant(d(1))) == Constant(d(0) % d(1)))
    assert(rem(Constant(l(0)), Constant(l(1))) == Constant(l(0) % l(1)))
    assert(rem(Constant(i(0)), arg[Int]) == Arithmetic("%", Constant(i(0)), arg[Int], INT_TYPE))

    assert(le(Constant(i(0)), Constant(i(1))) == Constant(i(0) <= i(1)))
    assert(le(Constant(f(0)), Constant(f(1))) == Constant(f(0) <= f(1)))
    assert(le(Constant(d(0)), Constant(d(1))) == Constant(d(0) <= d(1)))
    assert(le(Constant(l(0)), Constant(l(1))) == Constant(l(0) <= l(1)))
    assert(le(Constant(i(0)), arg[Int]) == Arithmetic("<=", Constant(i(0)), arg[Int], BOOLEAN_TYPE))
    assert(not(le(Constant(i(0)), arg[Int])) ==
      Arithmetic(">", Constant(i(0)), arg[Int], BOOLEAN_TYPE))

    assert(ge(Constant(i(0)), Constant(i(1))) == Constant(i(0) >= i(1)))
    assert(ge(Constant(f(0)), Constant(f(1))) == Constant(f(0) >= f(1)))
    assert(ge(Constant(d(0)), Constant(d(1))) == Constant(d(0) >= d(1)))
    assert(ge(Constant(l(0)), Constant(l(1))) == Constant(l(0) >= l(1)))
    assert(ge(Constant(i(0)), arg[Int]) == Arithmetic(">=", Constant(i(0)), arg[Int], BOOLEAN_TYPE))
    assert(not(ge(Constant(i(0)), arg[Int])) ==
      Arithmetic("<", Constant(i(0)), arg[Int], BOOLEAN_TYPE))

    assert(lt(Constant(i(0)), Constant(i(1))) == Constant(i(0) < i(1)))
    assert(lt(Constant(f(0)), Constant(f(1))) == Constant(f(0) < f(1)))
    assert(lt(Constant(d(0)), Constant(d(1))) == Constant(d(0) < d(1)))
    assert(lt(Constant(l(0)), Constant(l(1))) == Constant(l(0) < l(1)))
    assert(lt(Constant(i(0)), arg[Int]) == Arithmetic("<", Constant(i(0)), arg[Int], BOOLEAN_TYPE))
    assert(not(lt(Constant(i(0)), arg[Int])) ==
      Arithmetic(">=", Constant(i(0)), arg[Int], BOOLEAN_TYPE))

    assert(gt(Constant(i(0)), Constant(i(1))) == Constant(i(0) > i(1)))
    assert(gt(Constant(f(0)), Constant(f(1))) == Constant(f(0) > f(1)))
    assert(gt(Constant(d(0)), Constant(d(1))) == Constant(d(0) > d(1)))
    assert(gt(Constant(l(0)), Constant(l(1))) == Constant(l(0) > l(1)))
    assert(gt(Constant(i(0)), arg[Int]) == Arithmetic(">", Constant(i(0)), arg[Int], BOOLEAN_TYPE))
    assert(not(gt(Constant(i(0)), arg[Int])) ==
      Arithmetic("<=", Constant(i(0)), arg[Int], BOOLEAN_TYPE))

    assert(bitwiseAnd(Constant(i(0)), Constant(i(1))) == Constant(i(0) & i(1)))
    assert(bitwiseAnd(Constant(l(0)), Constant(l(1))) == Constant(l(0) & l(1)))
    assert(bitwiseAnd(Constant(i(0)), arg[Int]) ==
      Arithmetic("&", Constant(i(0)), arg[Int], INT_TYPE))

    assert(bitwiseOr(Constant(i(0)), Constant(i(1))) == Constant(i(0) | i(1)))
    assert(bitwiseOr(Constant(l(0)), Constant(l(1))) == Constant(l(0) | l(1)))
    assert(bitwiseOr(Constant(i(0)), arg[Int]) ==
      Arithmetic("|", Constant(i(0)), arg[Int], INT_TYPE))

    assert(bitwiseXor(Constant(i(0)), Constant(i(1))) == Constant(i(0) ^ i(1)))
    assert(bitwiseXor(Constant(l(0)), Constant(l(1))) == Constant(l(0) ^ l(1)))
    assert(bitwiseXor(Constant(i(0)), arg[Int]) ==
      Arithmetic("^", Constant(i(0)), arg[Int], INT_TYPE))

    assert(compareEqual(Constant(i(0)), Constant(i(1))) == Constant(i(0) == i(1)))
    assert(compareEqual(Constant(i(0)), Constant(i(0))) == Constant(i(0) == i(0)))
    assert(compareEqual(Constant(i(0)), arg[Int]) ==
      Arithmetic("==", Constant(i(0)), arg[Int], BOOLEAN_TYPE))
    assert(not(compareEqual(Constant(i(0)), arg[Int])) ==
      Arithmetic("!=", Constant(i(0)), arg[Int], BOOLEAN_TYPE))

    assert(compareNotEqual(Constant(i(0)), Constant(i(1))) == Constant(i(0) != i(1)))
    assert(compareNotEqual(Constant(i(0)), Constant(i(0))) == Constant(i(0) != i(0)))
    assert(compareNotEqual(Constant(i(0)), arg[Int]) ==
      Arithmetic("!=", Constant(i(0)), arg[Int], BOOLEAN_TYPE))
    assert(not(compareNotEqual(Constant(i(0)), arg[Int])) ==
      Arithmetic("==", Constant(i(0)), arg[Int], BOOLEAN_TYPE))

    assert(not(arg[Boolean]) == Arithmetic("!", arg[Boolean], Constant(false), BOOLEAN_TYPE))
  }

  test("cast operators") {
    assert(cast[AnyRef](cast[AnyRef](arg[java.lang.String])) == cast[AnyRef](arg[java.lang.String]))

    assert(cast[Float](Constant(i(0))) == Constant(i(0).toFloat))
    assert(cast[Float](Constant(d(0))) == Constant(d(0).toFloat))
    assert(cast[Float](Constant(l(0))) == Constant(l(0).toFloat))
    assert(cast[Float](cast[Double](arg[Float])) == arg[Float])
    assert(cast[Float](arg[Double]) == Cast(arg[Double], FLOAT_TYPE))

    assert(cast[Double](Constant(i(0))) == Constant(i(0).toDouble))
    assert(cast[Double](Constant(f(0))) == Constant(f(0).toDouble))
    assert(cast[Double](Constant(l(0))) == Constant(l(0).toDouble))
    assert(cast[Double](arg[Float]) == Cast(arg[Float], DOUBLE_TYPE))

    assert(cast[Long](Constant(i(0))) == Constant(i(0).toLong))
    assert(cast[Long](Constant(f(0))) == Constant(f(0).toLong))
    assert(cast[Long](Constant(d(0))) == Constant(d(0).toLong))
    assert(cast[Long](arg[Float]) == Cast(arg[Float], LONG_TYPE))

    assert(cast[Byte](Constant(i(0))) == Constant(i(0).toByte))
    assert(cast[Byte](cast[Int](arg[Byte])) == arg[Byte])
    assert(cast[Byte](arg[Int]) == Cast(arg[Int], BYTE_TYPE))

    assert(cast[Short](Constant(i(0))) == Constant(i(0).toShort))
    assert(cast[Short](cast[Int](arg[Short])) == arg[Short])
    assert(cast[Short](arg[Int]) == Cast(arg[Int], SHORT_TYPE))

    assert(cast[Char](Constant(i(0))) == Constant(i(0).toChar))
    assert(cast[Char](cast[Int](arg[Char])) == arg[Char])
    assert(cast[Char](arg[Int]) == Cast(arg[Int], CHAR_TYPE))

    assert(cast[Boolean](Constant(0)) == Constant(false))
    assert(cast[Boolean](Constant(1)) == Constant(true))
    assert(cast[Boolean](cast[Int](arg[Boolean])) == arg[Boolean])
    assert(cast[Boolean](arg[Int]) == Cast(arg[Int], BOOLEAN_TYPE))
    assert(cast[Boolean](ifElse(gt(arg[Int], Constant(0)), Constant(1), Constant(0))) ==
      ifElse(gt(arg[Int], Constant(0)), Constant(true), Constant(false)))

    assert(cast[Int](Constant(false)) == Constant(0))
    assert(cast[Int](Constant(true)) == Constant(1))

    assert(cast[Int](Constant(f(0))) == Constant(f(0).toInt))
    assert(cast[Int](Constant(d(0))) == Constant(d(0).toInt))
    assert(cast[Int](Constant(l(0))) == Constant(l(0).toInt))

    assert(cast[Int](Constant(i(0).toByte)) == Constant(i(0).toByte.toInt))
    assert(cast[Int](Constant(i(0).toShort)) == Constant(i(0).toShort.toInt))
    assert(cast[Int](Constant(i(0).toChar)) == Constant(i(0).toChar.toInt))
    assert(cast[Int](cast[Long](arg[Int])) == arg[Int])
    assert(cast[Int](arg[java.lang.Integer]) == Cast(arg[java.lang.Integer], INT_TYPE))
  }

  test("ifElse") {
    ifElse(Constant(true), arg[Int], Constant(0)) == arg[Int]
    ifElse(Constant(false), arg[Int], Constant(0)) == Constant(0)
    ifElse(gt(arg[Int], Constant(0)), Constant(true), Constant(false)) == gt(arg[Int], Constant(0))
    ifElse(gt(arg[Int], Constant(0)), Constant(false), Constant(true)) == le(arg[Int], Constant(0))
    ifElse(gt(arg[Int], Constant(0)), arg[Int], arg[Int]) == arg[Int]
    assert(ifElse(arg[Boolean], This(Type.getType(classOf[String])), Constant(null)).dataType ==
      Type.getType(classOf[String]))
    assert(ifElse(arg[Boolean], Constant(null), This(Type.getType(classOf[String]))).dataType ==
      Type.getType(classOf[String]))

    val ex = intercept[ByteCodeParserException]{
      ifElse(arg[Boolean], Constant(3), This(Type.getType(classOf[String])))
    }.getMessage
    assert(ex.contains("mismatches with right branch's data type"))
  }

  private def randomInts(count: Int) = (0 until count).map(_ => rand.nextInt()).toArray
  private def randomFloats(count: Int) = (0 until count).map(_ => rand.nextFloat()).toArray
  private def randomLongs(count: Int) = (0 until count).map(_ => rand.nextLong()).toArray
  private def randomDoubles(count: Int) = (0 until count).map(_ => rand.nextDouble()).toArray
}

