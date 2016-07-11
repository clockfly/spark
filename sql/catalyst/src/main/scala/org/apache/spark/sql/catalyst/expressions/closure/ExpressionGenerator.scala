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

import scala.language.implicitConversions

import org.apache.xbean.asm5.Type
import org.apache.xbean.asm5.Type._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Add, BitwiseAnd, BitwiseOr, BitwiseXor, Cast => CastExpression, CreateNamedStruct, Divide, EqualTo, Expression, ExtractValue, GreaterThan, GreaterThanOrEqual, If => IfExpression, IsNotNull, IsNull, LeafExpression, LessThan, LessThanOrEqual, Literal, Multiply, Not, Remainder, Subtract, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.closure.ByteCodeParser.{Argument, Arithmetic, Cast, Constant, FunctionCall, If, Node, Static, This, VOID}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.types.{AtomicType, DataType, NullType, ObjectType, StructType}

/**
 * Translate the closure to expression
 */
class ExpressionGenerator {

  import ExpressionGenerator._

  def generate(
    nodeTree: Node,
    argumentClass: Class[_],
    argumentSchema: StructType)
  : Seq[Expression] = {
    generate(nodeTree, Type.getType(argumentClass), argumentSchema)
  }

  /**
   * Creates an expression from the closure
   *
   * @param nodeTree Node tree generated after parsing the closure byte code.
   * @param argumentClass The class of the argument.
   * @param argumentSchema ScalaReflection.schemaFor[T] T is the argument type of closure
   * @return catalyst expression
   * @throws ClosureTranslationException
   */
  def generate(nodeTree: Node,
      argumentClass: Type,
      argumentSchema: StructType)
    : Seq[Expression] = {
    val atomic = argumentClass.isAtomic

    if (!atomic && !argumentClass.isCaseClass) {
      throw new ClosureTranslationException("ExpressionGenerator only support case class or " +
        s"atomic types, current type is: ${argumentClass.getClassName}")
    }

    def visit(node: Node): Expression = node match {
      case VOID => throw new ClosureTranslationException("VOID expression is not supported")
      case This(dataType) =>
        throw new ClosureTranslationException("Closure that reference free variable like " +
          s"THIS pointer is not supported (type: $dataType).")
      case Constant(constant: Char) => Literal(constant.toInt)
      case c@ Constant(constant) => Literal(constant)
      case Arithmetic(op, left, right, dataType) =>
        op match {
          case "+" => Add(visit(left), visit(right))
          case "-" => Subtract(visit(left), visit(right))
          case "*" => Multiply(visit(left), visit(right))
          case "/" =>
            checkCast(
              Divide(
                checkCast(visit(left), left.dataType, DOUBLE_TYPE),
                checkCast(visit(right), right.dataType, DOUBLE_TYPE)),
              DOUBLE_TYPE,
              dataType)
          case "<" => LessThan(visit(left), visit(right))
          case ">" => GreaterThan(visit(left), visit(right))
          case "<=" => LessThanOrEqual(visit(left), visit(right))
          case ">=" => GreaterThanOrEqual(visit(left), visit(right))
          case "%" => Remainder(visit(left), visit(right))
          case "&" => BitwiseAnd(visit(left), visit(right))
          case "|" => BitwiseOr(visit(left), visit(right))
          case "^" => BitwiseXor(visit(left), visit(right))
          case "!" => Not(visit(left))
          case "==" if left == Constant(null) => IsNull(visit(right))
          case "==" if right == Constant(null) => IsNull(visit(left))
          case "==" => EqualTo(visit(left), visit(right))
          case "!=" if left == Constant(null) => IsNotNull(visit(right))
          case "!=" if right == Constant(null) => IsNotNull(visit(left))
          case "!=" => Not(EqualTo(visit(left), visit(right)))
          case _ => throw new ClosureTranslationException(s"Unsupported operation $op")
        }
      case Cast(node, dataType) => checkCast(visit(node), node.dataType, dataType)
      case If(condition, left, right, dataType) =>
        IfExpression(visit(condition), visit(left), visit(right))
      case Argument(dataType) =>
        if (dataType.isAtomic) {
          Field(argumentSchema.fieldNames(0) :: Nil, dataType.sqlType)
        } else {
          Field(Seq.empty[String], dataType.sqlType)
        }
      case foo@ FunctionCall(_, clazz, method, args, dataType) if isStatic(foo) =>
        (clazz, method) match {
          case _ if isUnboxingMethod(clazz, method) =>
            checkCast(visit(args(0)), args(0).dataType, dataType)
          case _ if isBoxingMethod(clazz, method) =>
            visit(args(0))
          case _ =>
            throw new ClosureTranslationException(s"Unsupported static function $clazz/$method")
        }
      case foo@ FunctionCall(obj, clazz, method, _, dataType) if isGetter(foo) =>
        visit(obj) match {
          // Access sub-field of existing fields
          case Field(fieldPath, _) =>
            val subField = translateGetterToFieldName(clazz, method)
            Field(fieldPath :+ subField, dataType.sqlType)
          case _ =>
            throw new ClosureTranslationException(s"Unsupported getter method $clazz/$method on " +
              s"object (${obj})")
        }
      case FunctionCall(_, clazz, method, _, _) =>
        throw new ClosureTranslationException(s"Unsupported function call $clazz/$method")
      case other =>
        throw new ClosureTranslationException(s"Unsupported byte code parser tree $other")
    }

    // Makes sure all fields can be found in schema.
    val expression = resolveFields(visit(nodeTree), argumentSchema)

    // dataset.map(func: T => U) flatten the sub-fields of U if U is not primitive.
    // To be consistent with this, we should also flatten projection expression like $"a.b" to
    // expression list $"a.b.field1", $"a.b.field2"...
    val flattenExpressions: Seq[Expression] = expression match {
      // Top level object
      case create: CreateNamedStruct =>
        val (nameExprs, valExprs) =
        create.children.grouped(2).map { case Seq(name, value) => (name, value) }.toList.unzip
        valExprs
      case struct if struct.dataType.isInstanceOf[StructType] =>
        val dataType = struct.dataType.asInstanceOf[StructType]
        dataType.fieldNames.map { name =>
          ExtractValue(
            // Top-level row to should not be null, only its columns can be null.
            AssertNotNull(struct, Seq("top level non-flat input object")),
            Literal(name),
            caseSensitiveResolution)
        }
      case other => Seq(other)
    }

    // Add NPE checks
    flattenExpressions.map { expr =>
      expr.transformUp {
        case Field(name :: Nil, dataType) => UnresolvedAttribute(name :: Nil)
        case Field(nameParts, dataType) if nameParts.length > 1 =>
          val parent = UnresolvedAttribute(nameParts.slice(0, nameParts.length - 1))
          NPEOnNull(parent, UnresolvedAttribute(nameParts))
      }
    }
  }

  private val unboxingMethods = List(
    "scala.Predef$" -> "Long2long",
    "scala.Predef$" -> "Float2float",
    "scala.Predef$" -> "Double2double",
    "scala.Predef$" -> "Boolean2boolean",
    "scala.Predef$" -> "Short2short",
    "scala.Predef$" -> "Byte2byte",
    "scala.Predef$" -> "Integer2int"
  ).toSet

  private def isUnboxingMethod(className: String, method: String): Boolean = {
    unboxingMethods.contains((className -> method))
  }

  private val boxingMethods = List(
    "scala.Predef$" -> "long2Long",
    "scala.Predef$" -> "float2Float",
    "scala.Predef$" -> "double2Double",
    "scala.Predef$" -> "boolean2Boolean",
    "scala.Predef$" -> "short2Short",
    "scala.Predef$" -> "byte2Byte",
    "scala.Predef$" -> "int2Integer"
  ).toSet

  private def isBoxingMethod(className: String, method: String): Boolean = {
    boxingMethods.contains(className -> method)
  }

  private def isStatic(foo: FunctionCall): Boolean = {
    foo match {
      case _ if foo.obj == Constant(null) => true
      // Companion object access
      case FunctionCall(Static(_, "MODULE$", _), _, _, _, _) => true
      case _ => false
    }
  }

  private def translateGetterToFieldName(clazz: String, getter: String): String = clazz match {
    case "scala.Tuple" if getter.matches("^_[0-9][0-9]?\\$.*") =>
      getter.substring(0, getter.indexOf("$"))
    case _ => getter
  }

  private def isGetter(foo: FunctionCall): Boolean = {
    !isStatic(foo) &&
      foo.arguments.length == 0 &&
      (foo.dataType.isAtomic || foo.dataType.isCaseClass)
  }

  // Make sure all fields can be resolved in schema
  private def resolveFields(expression: Expression, schema: StructType): Expression = {
    expression.transformDown {
      case IsNull(Field(Nil, _)) =>
        // Top level object is always not null
        Literal(false)
      case IsNotNull(Field(Nil, _)) =>
        Literal(true)
      case Field(Nil, dataType: ObjectType) =>
        val fields = schema.fields.flatMap { field =>
          Literal(field.name) :: Field(field.name :: Nil, field.dataType) :: Nil
        }
        CreateNamedStruct(fields)
      case f@ Field(nameParts, dataType: ObjectType) if nameParts.length > 0 =>
        fieldSchema(schema, nameParts) match {
          case structType: StructType =>
            Field(nameParts, structType)
          case actualType =>
            throw new ClosureTranslationException(s"Failed to resolve field $f because " +
              s"required data type $dataType is not compatible with actual type ${actualType}")
        }
      case field@ Field(nameParts, dataType: AtomicType) =>
        val schemaType = fieldSchema(schema, nameParts)
        if (dataType == schemaType) {
          field
        } else {
          throw new ClosureTranslationException(s"Failed to resolve field $field because " +
            s"required data type $dataType mismatch with actual type ${schemaType}")
        }
      case field@ Field(nameParts, dataType: StructType) =>
        val schemaType = fieldSchema(schema, nameParts)
        if (dataType == schemaType) {
          field
        } else {
          throw new ClosureTranslationException(s"Failed to resolve field $field because " +
            s"required data type $dataType mismatch with actual type ${schemaType}")
        }
      case f@ Field(_, dataType) =>
        throw new ClosureTranslationException(s"Failed to resolve field $f because " +
          s"required data type $dataType mismatch with schema ${schema}")
    }.transformUp {
      case ifExpr@ IfExpression(condition, left, right) =>
        // Make sure the left and right's data type matches.
        val (newLeft, newRight) =
          (left.dataType, right.dataType) match {
            case (NullType, t2) => (CastExpression(left, t2), right)
            case (t1, NullType) => (left, CastExpression(right, t1))
            case (t1, t2) if t1 == t2 => (left, right)
            case _ =>
              throw new ClosureTranslationException(
                s"Failed to resolve If expression $ifExpr because left branch's data type " +
                  s"${left.dataType} mismatches with right branch's data type ${right.dataType}")
          }
        IfExpression(condition, newLeft, newRight)
    }
  }

  private def fieldSchema(rootSchema: StructType, fieldPath: Seq[String]): DataType = {
    var schema: DataType = rootSchema
    fieldPath.foreach { fieldName =>
      schema match {
        case struct: StructType if struct.fieldNames.contains(fieldName) =>
          schema = struct(fieldName).dataType
        case _ =>
          throw new ClosureTranslationException(s"Failed to find field $fieldName in $schema")
      }
    }
    schema
  }

  private def checkCast(expression: Expression, beforeCast: Type, afterCast: Type): Expression = {
    (beforeCast, afterCast) match {
      case (before, after) if before == after =>
        expression
      case (before, after) if before.isPrimitive && after.isPrimitive =>
        if (after == CHAR_TYPE) {
          // Char type need to be handled specially, as catalyst doesn't have this type.
          BitwiseAnd(CastExpression(expression, INT_TYPE.sqlType), Literal(0xffff))
        } else {
          CastExpression(expression, afterCast.sqlType)
        }
      case (before, after) if after.isBoxTypeOf(before) =>
        // boxing, no cast needed.
        expression
      case (before, after) if before.isBoxTypeOf(after) =>
        // unboxing, add NPE check.
        NPEOnNull(expression, expression)
      case (before, after) if before.isNull && !after.isPrimitive =>
        // Cast null to other object type is allowed.
        expression
      case (before, after) if after.isAssignableFrom(before) =>
        expression
      case _ =>
        throw new ClosureTranslationException(s"Cannot cast expression ${expression}($beforeCast)" +
          s" to ${afterCast}")
    }
  }
}

object ExpressionGenerator {

  implicit def typeToTypeOps(dataType: Type): TypeOps = new TypeOps(dataType)

  case class Field(
      nameParts: Seq[String],
      dataType: DataType) extends LeafExpression with Unevaluable {
    override def toString: String = s"'${nameParts.mkString(".")}"
    override def nullable: Boolean = true
  }

  // Throws exception if condition == null, else return child.
  // TODO: Optimize the performance...
  case class NPEOnNull(
      condition: Expression,
      child: Expression)
    extends Expression with CodegenFallback {

    override def children: Seq[Expression] = condition :: child :: Nil

    override def nullable: Boolean = false

    override def dataType: DataType = child.dataType

    override def eval(input: InternalRow): Any = {
      if (condition.eval(input) != null) {
        child.eval(input)
      } else {
        throw new NullPointerException
      }
    }
  }
}
