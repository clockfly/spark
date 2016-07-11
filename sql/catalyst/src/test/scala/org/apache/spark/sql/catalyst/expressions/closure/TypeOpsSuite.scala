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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.closure.TypeOps.UnSupportedTypeException
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, NullType, ObjectType, ShortType, StringType}

class TypeOpsSuite  extends SparkFunSuite {
  import TypeOpsSuite._

  test("type operations") {
    val dataTypes = Seq(
      // primitive types
      TypeInfo[Boolean](BOOLEAN_TYPE, getType[java.lang.Boolean], BooleanType, Primitive, Atomic),
      TypeInfo[Char](CHAR_TYPE, getType[java.lang.Character], IntegerType, Primitive, Atomic),
      TypeInfo[Byte](BYTE_TYPE, getType[java.lang.Byte], ByteType, Primitive, Atomic),
      TypeInfo[Short](SHORT_TYPE, getType[java.lang.Short], ShortType, Primitive, Atomic),
      TypeInfo[Int](INT_TYPE, getType[java.lang.Integer], IntegerType, Primitive, Atomic),
      TypeInfo[Float](FLOAT_TYPE, getType[java.lang.Float], FloatType, Primitive, Atomic),
      TypeInfo[Long](LONG_TYPE, getType[java.lang.Long], LongType, Primitive, Atomic),
      TypeInfo[Double](DOUBLE_TYPE, getType[java.lang.Double], DoubleType, Primitive, Atomic),

      // boxed types
      TypeInfo[java.lang.Boolean](getType[java.lang.Boolean], null, BooleanType, Box, Atomic),
      TypeInfo[java.lang.Character](getType[java.lang.Character], null, IntegerType, Box, Atomic),
      TypeInfo[java.lang.Byte](getType[java.lang.Byte], null, ByteType, Box, Atomic),
      TypeInfo[java.lang.Short](getType[java.lang.Short], null, ShortType, Box, Atomic),
      TypeInfo[java.lang.Integer](getType[java.lang.Integer], null, IntegerType, Box, Atomic),
      TypeInfo[java.lang.Float](getType[java.lang.Float], null, FloatType, Box, Atomic),
      TypeInfo[java.lang.Long](getType[java.lang.Long], null, LongType, Box, Atomic),
      TypeInfo[java.lang.Double](getType[java.lang.Double], null, DoubleType, Box, Atomic),

      // String type
      TypeInfo[java.lang.String](getType[java.lang.String], null, StringType, IsString, Atomic),

      // Null type
      TypeInfo[Null](getType[Null], null, NullType, IsNull, Atomic),

      // case class
      TypeInfo[A](getType[A], null, ObjectType(classOf[A]), CaseClass))

    dataTypes.foreach { typeInfo =>
      val ops = new TypeOps(typeInfo.dataType)
      assert(ops.isPrimitive == typeInfo.primitive)
      assert(ops.isAtomic == typeInfo.isAtomic)
      assert(ops.isBoxType == typeInfo.isBoxType)
      assert(ops.isString == typeInfo.isString)
      assert(ops.isNull == typeInfo.isNull)
      assert(ops.isCaseClass == typeInfo.isCaseClass)
      assert(ops.boxType == typeInfo.boxType)
      assert(ops.sqlType == typeInfo.sqlType)
      assert(ops.clazz == typeInfo.clazz)
      assert(ops.classTag == typeInfo.classTag)
    }
  }

  test("unsupported types") {
    // VOID
    intercept[UnSupportedTypeException] {
      val ops = new TypeOps(VOID_TYPE)
    }

    // ARRAY
    intercept[UnSupportedTypeException] {
      val ops = new TypeOps(getType[Array[String]])
    }

//    // METHOD
//    intercept[UnSupportedTypeException] {
//      val ops = new TypeOps(Type.getMethodType("Ljava/lang/String;)Ljava/lang/String;"))
//    }
  }

  private def getType[T: ClassTag] = Type.getType(classTag[T].runtimeClass)
}

object TypeOpsSuite {

  case class A(a1: Int, a2: String)

  case class TypeInfo[T](
    dataType: Type,
    boxType: Option[Type],
    sqlType: DataType,
    clazz: Class[T],
    classTag: ClassTag[T],
    primitive: Boolean,
    isAtomic: Boolean,
    isBoxType: Boolean,
    isString: Boolean,
    isNull: Boolean,
    isCaseClass: Boolean
  )

  val Primitive = "primitive"
  val Atomic = "atomic"
  val Box = "box"
  val IsString = "isString"
  val IsNull = "isNull"
  val CaseClass = "isCaseClass"

  object TypeInfo {
    def apply[T: ClassTag](
        dataType: Type,
        boxType: Type,
        sqlType: DataType,
        flags: String*): TypeInfo[T] = {

      val typeInfo = new TypeInfo[T](dataType, Option(boxType), sqlType,
        classTag[T].runtimeClass.asInstanceOf[Class[T]], classTag[T], false, false, false,
        false, false, false)

      flags.foldLeft(typeInfo) {
        case (typeInfo, Primitive) => typeInfo.copy(primitive = true)
        case (typeInfo, Atomic) => typeInfo.copy(isAtomic = true)
        case (typeInfo, Box) => typeInfo.copy(isBoxType = true)
        case (typeInfo, IsString) => typeInfo.copy(isString = true)
        case (typeInfo, IsNull) => typeInfo.copy(isNull = true)
        case (typeInfo, CaseClass) => typeInfo.copy(isCaseClass = true)
      }
    }
  }
}
