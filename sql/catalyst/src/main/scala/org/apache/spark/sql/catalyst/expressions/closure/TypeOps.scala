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

import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, NullType, ObjectType, ShortType, StringType}

// Extends ASM Type to provide some handy methods
class TypeOps(dataType: Type) {
import TypeOps._

  if (dataType == VOID_TYPE || dataType.getSort == METHOD || dataType.getSort == ARRAY) {
    throw new UnSupportedTypeException(dataType)
  }

  def isPrimitive: Boolean = primitiveTypes.contains(dataType)
  def isBoxType: Boolean = boxTypes.contains(dataType)
  def isString: Boolean = dataType == Type.getType(classOf[String])

  def isAtomic: Boolean = {
    isString || isNull || primitiveTypes.contains(dataType) || boxTypes.contains(dataType)
  }

  def boxType: Option[Type] = {
    dataType match {
      case BOOLEAN_TYPE => Some(Type.getType(classOf[java.lang.Boolean]))
      case BYTE_TYPE => Some(Type.getType(classOf[java.lang.Byte]))
      case CHAR_TYPE => Some(Type.getType(classOf[java.lang.Character]))
      case FLOAT_TYPE => Some(Type.getType(classOf[java.lang.Float]))
      case DOUBLE_TYPE => Some(Type.getType(classOf[java.lang.Double]))
      case INT_TYPE => Some(Type.getType(classOf[java.lang.Integer]))
      case LONG_TYPE => Some(Type.getType(classOf[java.lang.Long]))
      case SHORT_TYPE => Some(Type.getType(classOf[java.lang.Short]))
      case _ => None
    }
  }

  def sqlType: DataType = dataType match {
    case _ if isString => StringType
    case _ if isNull => NullType
    case _ if primitiveTypes.contains(dataType) => primitiveTypes(dataType).sqlType
    case _ if boxTypes.contains(dataType) => boxTypes(dataType).sqlType
    case _ => ObjectType(loadClass(dataType.getClassName))
  }

  def isNull: Boolean = clazz == classOf[Null]

  def isCaseClass: Boolean = classOf[Product].isAssignableFrom(clazz)

  def isBoxTypeOf(other: TypeOps): Boolean = {
    !isPrimitive && other.isPrimitive && Some(dataType) == other.boxType
  }

  def clazz: Class[_] = {
    primitiveTypes.get(dataType).map(_.clazz).getOrElse(loadClass(dataType.getClassName))
  }

  def isAssignableFrom(other: TypeOps): Boolean = clazz.isAssignableFrom(other.clazz)

  def classTag: ClassTag[_] = primitiveTypes.get(dataType).map(_.classTag).getOrElse {
    ClassTag(clazz)
  }

  private def loadClass(className: String): Class[_] = {
    Thread.currentThread().getContextClassLoader.loadClass(className)
  }
}

object TypeOps {

  class UnSupportedTypeException(dataType: Type)
    extends ClosureTranslationException(dataType.toString)

  private class TypeInfo(val sqlType: DataType, val classTag: ClassTag[_]) {
    def clazz: Class[_] = classTag.runtimeClass
  }

  private val primitiveTypes = Map(
    BOOLEAN_TYPE -> new TypeInfo(BooleanType, classTag[Boolean]),
    BYTE_TYPE -> new TypeInfo(ByteType, classTag[Byte]),
    // TODO: Add support for Char type in spark sql.
    CHAR_TYPE -> new TypeInfo(IntegerType, classTag[Char]),
    FLOAT_TYPE -> new TypeInfo(FloatType, classTag[Float]),
    DOUBLE_TYPE -> new TypeInfo(DoubleType, classTag[Double]),
    INT_TYPE -> new TypeInfo(IntegerType, classTag[Int]),
    LONG_TYPE -> new TypeInfo(LongType, classTag[Long]),
    SHORT_TYPE -> new TypeInfo(ShortType, classTag[Short])
  )

  private val boxTypes = Map(
    Type.getType(classOf[java.lang.Boolean]) ->
      new TypeInfo(BooleanType, classTag[java.lang.Boolean]),
    // TODO: Add support for Char type in spark sql.
    Type.getType(classOf[java.lang.Character]) ->
      new TypeInfo(IntegerType, classTag[java.lang.Character]),
    Type.getType(classOf[java.lang.Byte]) -> new TypeInfo(ByteType, classTag[java.lang.Byte]),
    Type.getType(classOf[java.lang.Float]) -> new TypeInfo(FloatType, classTag[java.lang.Float]),
    Type.getType(classOf[java.lang.Double]) -> new TypeInfo(DoubleType, classTag[java.lang.Double]),
    Type.getType(classOf[java.lang.Integer]) ->
      new TypeInfo(IntegerType, classTag[java.lang.Integer]),
    Type.getType(classOf[java.lang.Long]) -> new TypeInfo(LongType, classTag[java.lang.Long]),
    Type.getType(classOf[java.lang.Short]) -> new TypeInfo(ShortType, classTag[java.lang.Short])
  )

  def primitives: List[Type] = primitiveTypes.keys.toList.sortBy(_.getSort)
}
