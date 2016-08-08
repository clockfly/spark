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

import org.apache.xbean.asm5.{ClassReader, ClassVisitor, Label, MethodVisitor, Type}
import org.apache.xbean.asm5.Opcodes._
import org.apache.xbean.asm5.Type._
import org.apache.xbean.asm5.tree.{AbstractInsnNode, FieldInsnNode, FrameNode, IincInsnNode, InsnList, InsnNode, IntInsnNode, JumpInsnNode, LabelNode, LdcInsnNode, LineNumberNode, MethodInsnNode, MethodNode, TypeInsnNode, VarInsnNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.closure.DSL._

object ByteCodeParser {

  // OPcode names. The array index is the Opcode.
  // Opcode list: https://en.wikipedia.org/wiki/Java_bytecode_instruction_listings
  private val OPCODES =
    Array("NOP", "ACONST_NULL", "ICONST_M1", "ICONST_0", "ICONST_1",
      "ICONST_2", "ICONST_3", "ICONST_4", "ICONST_5", "LCONST_0", "LCONST_1", "FCONST_0",
      "FCONST_1", "FCONST_2", "DCONST_0", "DCONST_1", "BIPUSH", "SIPUSH", "LDC", "", "",
      "ILOAD", "LLOAD", "FLOAD", "DLOAD", "ALOAD", "", "", "", "", "", "", "", "", "", "",
      "", "", "", "", "", "", "", "", "", "", "IALOAD", "LALOAD", "FALOAD", "DALOAD", "AALOAD",
      "BALOAD", "CALOAD", "SALOAD", "ISTORE", "LSTORE", "FSTORE", "DSTORE", "ASTORE",
      "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
      "IASTORE", "LASTORE", "FASTORE", "DASTORE", "AASTORE", "BASTORE", "CASTORE", "SASTORE",
      "POP", "POP2", "DUP", "DUP_X1", "DUP_X2", "DUP2", "DUP2_X1", "DUP2_X2", "SWAP", "IADD",
      "LADD", "FADD", "DADD", "ISUB", "LSUB", "FSUB", "DSUB", "IMUL", "LMUL", "FMUL", "DMUL",
      "IDIV", "LDIV", "FDIV", "DDIV", "IREM", "LREM", "FREM", "DREM", "INEG", "LNEG", "FNEG",
      "DNEG", "ISHL", "LSHL", "ISHR", "LSHR", "IUSHR", "LUSHR", "IAND", "LAND", "IOR", "LOR",
      "IXOR", "LXOR", "IINC", "I2L", "I2F", "I2D", "L2I", "L2F", "L2D", "F2I", "F2L", "F2D",
      "D2I", "D2L", "D2F", "I2B", "I2C", "I2S", "LCMP", "FCMPL", "FCMPG", "DCMPL", "DCMPG",
      "IFEQ", "IFNE", "IFLT", "IFGE", "IFGT", "IFLE", "IF_ICMPEQ", "IF_ICMPNE", "IF_ICMPLT",
      "IF_ICMPGE", "IF_ICMPGT", "IF_ICMPLE", "IF_ACMPEQ", "IF_ACMPNE", "GOTO", "JSR", "RET",
      "TABLESWITCH", "LOOKUPSWITCH", "IRETURN", "LRETURN", "FRETURN", "DRETURN", "ARETURN",
      "RETURN", "GETSTATIC", "PUTSTATIC", "GETFIELD", "PUTFIELD", "INVOKEVIRTUAL",
      "INVOKESPECIAL", "INVOKESTATIC", "INVOKEINTERFACE", "INVOKEDYNAMIC", "NEW", "NEWARRAY",
      "ANEWARRAY", "ARRAYLENGTH", "ATHROW", "CHECKCAST", "INSTANCEOF", "MONITORENTER",
      "MONITOREXIT", "", "MULTIANEWARRAY", "IFNULL", "IFNONNULL")

  def opString(opcode: Int): Option[String] = {
    if (opcode > 0 && opcode < OPCODES.length) {
      Some(OPCODES(opcode))
    } else {
      None
    }
  }

  class UnsupportedOpcodeException(
    opcode: Int,
    message: String = "")
    extends ByteCodeParserException(
      s"Unsupported opcode ${opString(opcode).getOrElse(opcode.toString)}, $message")

  // TODO: Support GETFIELD, for scala companion object constant reference...
  private val UnsupportedOpcodes = Set(
    // InvokeDynamicInsnNode
    INVOKEDYNAMIC,
    // FieldInsnNode
    PUTFIELD, PUTSTATIC, GETFIELD,
    // MultiANewArrayInsnNode
    MULTIANEWARRAY,
    // JumpInsnNode, JSR is not used by Java compile since JDK6.
    JSR,
    // VarInsnNode, RET is not used by Java compile since JDK6.
    RET,
    // TypeInsnNode
    NEW, INSTANCEOF, ANEWARRAY,
    // IntInsnNode
    NEWARRAY,
    // InsnNode
    ISHL, LSHL, ISHR, LSHR, IUSHR, LUSHR,
    ARRAYLENGTH, IALOAD, LALOAD, FALOAD, DALOAD, AALOAD, BALOAD, CALOAD, SALOAD,
    IASTORE, LASTORE, FASTORE, DASTORE, AASTORE, BASTORE, CASTORE, SASTORE,
    ATHROW,
    MONITORENTER, MONITOREXIT,
    // TableSwitchInsnNode
    TABLESWITCH,
    // LookupSwitchInsnNode
    LOOKUPSWITCH
  )

  private def isPseudo(instruction: AbstractInsnNode): Boolean = instruction match {
    case label: LabelNode => true
    case lineNumber: LineNumberNode => true
    case frame: FrameNode => true
    case _ => false
  }

  private def isReturn(instruction: AbstractInsnNode): Boolean = instruction.getOpcode match {
    case DRETURN | FRETURN | IRETURN | LRETURN | ARETURN | RETURN => true
    case _ => false
  }

  private def isLoadInstruction(inst: AbstractInsnNode): Boolean = inst.getOpcode match {
    case ILOAD | LLOAD | FLOAD | DLOAD | ALOAD => true
    case _ => false
  }

  private class MethodTracer(
      method: MethodNode,
      trace: Boolean = true) extends Logging {

    private var labelIndex = 0
    private var labels = Map.empty[Label, Int]
    private val msg = new StringBuilder

    msg.append(
      s"""
         |ByteCode of closure method ${method.name}:
         |===============================================
         |${method.instructions.toArray.map(instructionString(_)).mkString("\n")}
         |
         |Start tracing closure method ${method.name}:
         |===============================================
         |""".stripMargin)

    def trace(stack: List[Node], instruction: AbstractInsnNode): Unit = {
      val stackString = if (stack.nonEmpty) s"stack: ${stack.mkString(",")}\n" else ""
      msg.append(s"$stackString${instructionString(instruction)}\n")
    }

    def flush(): Unit = {
      logTrace(msg.toString())
      msg.clear()
    }

    private def instructionString(instruction: AbstractInsnNode): String = {
      val opcode = instruction.getOpcode
      val name = opString(opcode).getOrElse(instruction.getClass.getSimpleName.replace("Node", ""))
      instruction match {
        case f: FieldInsnNode => s"  $name ${f.owner} ${f.name} ${f.desc}"
        case j: JumpInsnNode => s"  $name ${label(j.label.getLabel)}"
        case l: LdcInsnNode => s"  $name ${l.cst}"
        case f: MethodInsnNode => s"  $name ${f.owner} ${f.name} ${f.desc}"
        case l: LabelNode => s"${label(l.getLabel)}"
        case i: IntInsnNode => s"  $name ${i.operand}"
        case t: TypeInsnNode => s"  $name ${t.desc}"
        case i: IincInsnNode => s"  $name ${i.`var`} ${i.incr}"
        case v: VarInsnNode => s"  $name ${v.`var`}"
        case _ => s"  $name"
      }
    }

    private def label(label: Label): String = {
      if (!labels.contains(label)) {
        labels += label -> labelIndex
        labelIndex += 1
      }
      s"L${labels(label)}:"
    }
  }
}

/**
 * Parses the closure and generate a Node tree to represent the computation of the closure.
 *
 * For example, closure (v: )
 * {{{
 *   // Scala
 *   (v: Int) => { v > 0 }
 * }}}
 *
 * is translated to:
 * {{{
 *   Arithmetic[Z](>)
 *     Argument[I]
 *     Constant[I](0)
 * }}}
 *
 */
class ByteCodeParser {

  import org.apache.spark.sql.catalyst.expressions.closure.ByteCodeParser._

  /**
   * Parses the closure and generate a Node tree to represent the computation of the closure.
   *
   * @param closure closure of single input argument and single return value
   * @param argumentClass input argument class of closure
   * @return root Node of the Node tree.
   * @throws ByteCodeParserException
   */
  def parse(closure: Class[_], argumentClass: Class[_]): Node = {
    // The Scala closure use apply as method name, sometimes it has a suffix, like apply$mcI$sp
    // The Java closure we use like MapFunction use call as method name.
    // The pattern match here make sure we only parse the Scala closure method or Java closure
    // method. Other methods are ignored.
    val defaultNamePattern = "call|apply(\\$mc.*\\$sp)?"
    parse(closure, argumentClass, defaultNamePattern)
  }

  private def parse(closure: Class[_], argumentClass: Class[_], methodNamePattern: String): Node = {
    // Scala compiler may automatically generates multiple apply methods with different argument
    // type, like apply(obj: Object), apply(v: Int), apply$mcI$sp(v: Int). Some apply method
    // may delegates call to another apply method. Here we tries to gather as more candidate apply
    // method as possible during the scan of closure byte code. We will do disambiguation later.
    var candidateMethods = List.empty[MethodNode]
    val closureResource = Thread.currentThread().getContextClassLoader
      .getResourceAsStream(closure.getName.replace('.', '/') + ".class")
    val reader = new ClassReader(closureResource)
    reader.accept(new ClassVisitor(ASM5, null) {
      override def visitMethod(
        access: Int,
        name: String,
        desc: String,
        signature: String,
        exceptions: Array[String])
      : MethodVisitor = {
        if (isApplyMethod(argumentClass, name, desc)) {
          val method = new MethodNode(access, name, desc, signature, exceptions)
          candidateMethods = method :: candidateMethods
          method
        } else {
          // null means MethodVisitor is not defined, thus, byte code of this method will be
          // skipped during scan to improve performance.
          null
        }
      }

      // Check whether it is a valid apply method, with requirements:
      // 1. Name matches "apply" or "apply$mc.*$sp".
      // 2. Single argument function.
      // 3. Argument's type matches the expected type.
      private def isApplyMethod(
          argumentType: Class[_],
          name: String,
          signature: String): Boolean = {
        val argumentTypes = Type.getArgumentTypes(signature)

        argumentTypes.length == 1 &&
          argumentTypes(0).getClassName == argumentType.getName &&
          name.matches(methodNamePattern)
      }
    }, 0)

    // Disambiguation
    val applyMethods = resolve(candidateMethods)

    if (applyMethods.isEmpty) {
      // Throws error if there is no apply method or argument type mismatches expected type.
      throw new ByteCodeParserException(s"Cannot find an apply method in closure " +
        s"${closure.getName}. The expected argument type is: ${argumentClass.getName}")
    } else if (applyMethods.length > 1) {
      // Scala compiler may generates multiple apply methods with same input argument type but
      // different return type.
      throw new ByteCodeParserException(s"Found multiple ambiguous apply methods with signature " +
        s"${applyMethods.map(_.desc).mkString(", ")}")
    }
    analyze(closure, applyMethods.head)
  }

  // Proxy method delegates the call to another method.
  // Example:
  // {{{
  //   apply(v: AnyRef): AnyRef = apply(v.toInt)
  // }}}
  private def isProxyMethod(method: MethodNode): Boolean = {
    val instructions = method.instructions.toArray.toList.filterNot(isPseudo(_))
    instructions match {
      case (load0: VarInsnNode)::(load1: VarInsnNode)::(invoke: MethodInsnNode)::ret::Nil
        if load0.getOpcode == ALOAD && load0.`var` == 0 &&
          isLoadInstruction(load1) && load1.`var` == 1 &&
          isReturn(ret) &&
          invoke.getOpcode == INVOKEVIRTUAL &&
          getArgumentTypes(invoke.desc).sameElements(getArgumentTypes(method.desc)) =>
        true
      case _ =>
        false
    }
  }

  private def resolve(candidates: List[MethodNode]): List[MethodNode] = {
    candidates.filterNot(isProxyMethod(_))
  }

  // Translates the apply method to Node tree.
  private def analyze(closure: Class[_], applyMethod: MethodNode): Node = {
    if (applyMethod.tryCatchBlocks.size() != 0) {
      throw new ByteCodeParserException("try...catch... is not supported in ByteCodeParser")
    }

    val argumentType = getArgumentTypes(applyMethod.desc)(0)
    // JVM treats boolean/byte/short/char as int internally. We should cast the Argument to int
    // if the argument type is one of Boolean, Byte, Char, Short
    val argument = {
      val inputArgument = Argument(argumentType)
      argumentType match {
        case BOOLEAN_TYPE | BYTE_TYPE | SHORT_TYPE | CHAR_TYPE => cast(inputArgument, INT_TYPE)
        case _ => inputArgument
      }
    }

    // To simulate the local variables in apply method like "var x = 0".
    var localVars = Map.empty[Int, Node]
    localVars += 0 -> This(getType(closure))
    localVars += 1 -> argument

    val tracer = new MethodTracer(applyMethod, trace = true)

    // invoke instructions starting from startIndex
    def invoke(
        instructions: InsnList,
        startIndex: Int,
        inputStack: List[Node],
        inputLocalVars: Map[Int, Node]): Node = {
      var result: Option[Node] = None
      var index = startIndex
      var localVars = inputLocalVars

      var stack = inputStack

      def pop(): Node = {
        val top = stack.head
        stack = stack.tail
        top
      }

      def push(node: Node): Unit = {
        stack = node :: stack
      }

      while (index < instructions.size() && result.isEmpty) {
        val node = instructions.get(index)
        val opcode = node.getOpcode
        if (ByteCodeParser.UnsupportedOpcodes.contains(opcode)) {
          throw new UnsupportedOpcodeException(opcode)
        }

        tracer.trace(stack, node)

        node match {
          // non-static/static function call instructions
          case method: MethodInsnNode =>
            method.getOpcode match {
              case INVOKEVIRTUAL | INVOKESTATIC | INVOKESPECIAL | INVOKEINTERFACE =>
                val className = getObjectType(method.owner).getClassName
                val methodName = method.name
                val argumentLength = getArgumentTypes(method.desc).length
                val returnType = getReturnType(method.desc)
                val arguments = (0 until argumentLength).toList.map(_ => pop()).reverse
                val obj = if (method.getOpcode == INVOKESTATIC) {
                  Constant(null)
                } else {
                  pop()
                }
                push(FunctionCall(obj, className, methodName, arguments, returnType))
            }
          // non-static/static field access instructions.
          case field: FieldInsnNode =>
            field.getOpcode match {
              case GETSTATIC =>
                val className = getObjectType(field.owner).getClassName
                val dataType = getType(field.desc)
                push(StaticField(className, field.name, dataType))
              case _ => throw new UnsupportedOpcodeException(opcode)
            }
          // instructions that has a integer as operand
          case intInstruction: IntInsnNode =>
            intInstruction.getOpcode match {
              case BIPUSH | SIPUSH => push(Constant(intInstruction.operand))
              case _ => throw new UnsupportedOpcodeException(opcode)
            }
          // instruction that takes a type descriptor as parameter
          case typeInstruction: TypeInsnNode =>
            typeInstruction.getOpcode match {
              case CHECKCAST => // skip
                val input = pop()
                push(cast(input, getObjectType(typeInstruction.desc)))
              case _ => throw new UnsupportedOpcodeException(opcode)
            }
          // Increments an integer in local var.
          case inc: IincInsnNode =>
            val index = inc.`var`
            val increase = inc.incr
            val localVar = localVars(index)
            localVars += index -> plus(localVar, Constant(increase))
          // Jump instructions
          case jump: JumpInsnNode =>
            // comparator: <, >, ==, <=, >=
            def compareAndJump(comparator: (Node, Node) => Node): Node = {
              val right = pop()
              val left = pop()

              if (jump.label == instructions.get(index + 1)) {
                // Jump to immediate next instruction
                invoke(instructions, instructions.indexOf(jump.label), stack, localVars)
              } else {
                // If the condition is a - b > 0, translates it to a > b
                val condition = left match {
                  case a @ Arithmetic("-", _, _, _) if right == Constant(0) =>
                    comparator(a.left, a.right)
                  case _ => comparator(left, right)
                }

                ifElse(condition,
                  invoke(instructions, instructions.indexOf(jump.label), stack, localVars),
                  // Otherwise, jump to next instruction
                  invoke(instructions, index + 1, stack, localVars)
                )
              }
            }

            if (instructions.indexOf(jump.label) <= index) {
              throw new UnsupportedOpcodeException(jump.getOpcode, "Backward jump is not " +
                "supported because it may create a loop")
            }

            jump.getOpcode match {
              case IF_ICMPEQ | IF_ACMPEQ =>
                result = Some(compareAndJump(compareEqual))
              case IF_ICMPNE | IF_ACMPNE =>
                result = Some(compareAndJump(compareNotEqual))
              case IF_ICMPLT =>
                result = Some(compareAndJump(lt))
              case IF_ICMPGT =>
                result = Some(compareAndJump(gt))
              case IF_ICMPLE =>
                result = Some(compareAndJump(le))
              case IF_ICMPGE =>
                result = Some(compareAndJump(ge))
              case IFNULL =>
                push(Constant(null))
                result = Some(compareAndJump(compareEqual))
              case IFNONNULL =>
                push(Constant(null))
                result = Some(compareAndJump(compareNotEqual))
              case IFEQ =>
                push(Constant(0))
                result = Some(compareAndJump(compareEqual))
              case IFNE =>
                push(Constant(0))
                result = Some(compareAndJump(compareNotEqual))
              case IFLT =>
                push(Constant(0))
                result = Some(compareAndJump(lt))
              case IFGT =>
                push(Constant(0))
                result = Some(compareAndJump(gt))
              case IFLE =>
                push(Constant(0))
                result = Some(compareAndJump(le))
              case IFGE =>
                push(Constant(0))
                result = Some(compareAndJump(ge))
              case GOTO =>
                index = instructions.indexOf(jump.label) - 1
              case _ => throw new UnsupportedOpcodeException(opcode)
            }
          // load constant instructions to stack
          case load: LdcInsnNode =>
            val constant = load.cst
            constant match {
              case i: java.lang.Integer => push(Constant[Int](i))
              case f: java.lang.Float => push(Constant[Float](f))
              case d: java.lang.Double => push(Constant[Double](d))
              case l: java.lang.Long => push(Constant[Long](l))
              case str: java.lang.String => push(Constant[String](str))
              case other =>
                throw new UnsupportedOpcodeException(load.getOpcode, s"LDC only supports type " +
                  s"Int, Float, Double, Long and String, current type is ${other.getClass.getName}")
            }
          // load/store value from/to local variable.
          case localVar: VarInsnNode =>
            val index = localVar.`var`
            localVar.getOpcode match {
              case ILOAD | LLOAD | FLOAD | DLOAD | ALOAD =>
                push(localVars(index))
              case ISTORE | LSTORE | FSTORE | DSTORE | ASTORE =>
                val top = pop()
                localVars += index -> top
              case _ => throw new UnsupportedOpcodeException(opcode)
            }
          // Instructions that don't have an operand.
          case op: InsnNode =>
            op.getOpcode match {
              case NOP => // Skip
              // Load constants to stack
              case ACONST_NULL => push(Constant(null))
              case ICONST_M1 => push(Constant(-1))
              case ICONST_0 => push(Constant(0))
              case ICONST_1 => push(Constant(1))
              case ICONST_2 => push(Constant(2))
              case ICONST_3 => push(Constant(3))
              case ICONST_4 => push(Constant(4))
              case ICONST_5 => push(Constant(5))
              case LCONST_0 => push(Constant(0L))
              case LCONST_1 => push(Constant(1L))
              case FCONST_0 => push(Constant(0F))
              case FCONST_1 => push(Constant(1F))
              case FCONST_2 => push(Constant(2F))
              case DCONST_0 => push(Constant(0D))
              case DCONST_1 => push(Constant(1D))
              // Arithmetic operations
              case IADD | LADD | FADD | DADD =>
                val right = pop()
                val left = pop()
                push(plus(left, right))
              case ISUB | LSUB | FSUB | DSUB =>
                val right = pop()
                val left = pop()
                push(minus(left, right))
              case IMUL | LMUL | FMUL | DMUL =>
                val right = pop()
                val left = pop()
                push(mul(left, right))
              case IDIV | LDIV | FDIV | DDIV =>
                val right = pop()
                val left = pop()
                push(div(left, right))
              case IREM | LREM | FREM | DREM =>
                val right = pop()
                val left = pop()
                push(rem(left, right))
              case INEG =>
                val top = pop()
                push(minus(Constant(0), top))
              case LNEG =>
                val top = pop()
                push(minus(Constant(0L), top))
              case FNEG =>
                val top = pop()
                push(minus(Constant(0F), top))
              case DNEG =>
                val top = pop()
                push(minus(Constant(0D), top))
              case IAND | LAND =>
                val right = pop()
                val left = pop()
                push(bitwiseAnd(left, right))
              case IOR | LOR =>
                val right = pop()
                val left = pop()
                push(bitwiseOr(left, right))
              case IXOR | LXOR =>
                val right = pop()
                val left = pop()
                push(bitwiseXor(left, right))
              // Cast operations
              case I2L | F2L | D2L =>
                push(cast[Long](pop()))
              case L2I | F2I | D2I =>
                push(cast[Int](pop()))
              case I2F | L2F | D2F =>
                push(cast[Float](pop()))
              case I2D | L2D | F2D =>
                push(cast[Double](pop()))
              case I2B => push(cast[Int](cast[Byte](pop())))  // Sign-extended to an int
              case I2S => push(cast[Int](cast[Short](pop()))) // Sign-extended to an int
              case I2C => push(cast[Int](cast[Char](pop()))) // Zero-extended to an int
              // long/float/double compare and jump instructions.
              case LCMP | FCMPL | FCMPG | DCMPL | DCMPG =>
                val jump = instructions.get(index + 1).getOpcode match {
                  case IFEQ | IFNE | IFLT | IFGT | IFLE | IFGE =>
                    instructions.get(index + 1).asInstanceOf[JumpInsnNode]
                  case _ =>
                    throw new UnsupportedOpcodeException(
                      opcode,
                      s"${opString(op.getOpcode).getOrElse(opcode.toString)} need be followed " +
                        s"by a jump instruction like IFEQ, IFNE, IFLT, IFGT, IFLE, IFGE")
                }

                // Rewrite the op to reuse the code for integer compare and jump.
                jump.getOpcode match {
                  case IFEQ => jump.setOpcode(IF_ICMPEQ)
                  case IFNE => jump.setOpcode(IF_ICMPNE)
                  case IFLT => jump.setOpcode(IF_ICMPLT)
                  case IFGT => jump.setOpcode(IF_ICMPGT)
                  case IFLE => jump.setOpcode(IF_ICMPLE)
                  case IFGE => jump.setOpcode(IF_ICMPGE)
                }
              // stack operations.
              case POP | POP2 | DUP | DUP2 | DUP_X1 | DUP_X2 | DUP2_X1 | DUP2_X2 | SWAP =>
                // Each data type has a category, which affects the behavior of stack operations.
                // JVM Category 2 types: Long, Double.
                // JVM Category 1 types: Boolean, Byte, Char,Short, Int, Float, Reference,
                // ReturnAddress.
                // For example, POP2 only pop 1 category 2 data type, but pops 2 category 1 data
                // type.
                //
                // @See https://docs.oracle.com/javase/specs/jvms/se7/html/jvms-2.html#jvms-2.11.1
                val stackCategories = stack.map(_.dataType).map {
                  case LONG_TYPE | DOUBLE_TYPE => 2
                  case _ => 1
                }.slice(0, 4) // Stack operations like DUP2_X2 at max use 4 stack slots.

                // @See https://docs.oracle.com/javase/specs/jvms/se7/html/jvms-6.html to find
                // how these stack operations work.
                (op.getOpcode, stackCategories) match {
                  case (POP, 1::_) => pop()
                  case (POP2, 1::1::_) =>
                    pop()
                    pop()
                  case (POP2, 2::_) => pop()
                  case (DUP, 1::_) =>
                    val top = pop()
                    push(top)
                    push(top)
                  case (DUP2, 1::1::_) =>
                    val first = pop()
                    val second = pop()
                    push(second)
                    push(first)
                    push(second)
                    push(first)
                  case (DUP2, 2::_) =>
                    val top = pop()
                    push(top)
                    push(top)
                  case (DUP_X1, 1::1::_) =>
                    val first = pop()
                    val second = pop()
                    push(first)
                    push(second)
                    push(first)
                  case (DUP_X2, 1::1::1::_) =>
                    val first = pop()
                    val second = pop()
                    val third = pop()
                    push(first)
                    push(third)
                    push(second)
                    push(first)
                  case (DUP_X2, 1::2::_) =>
                    val first = pop()
                    val second = pop()
                    push(first)
                    push(second)
                    push(first)
                  case (DUP2_X1, 1::1::1::_) =>
                    val first = pop()
                    val second = pop()
                    val third = pop()
                    push(second)
                    push(first)
                    push(third)
                    push(second)
                    push(first)
                  case (DUP2_X1, 2::1::_) =>
                    val first = pop()
                    val second = pop()
                    push(first)
                    push(second)
                    push(first)
                  case (DUP2_X2, 1::1::1::1::_) =>
                    val first = pop()
                    val second = pop()
                    val third = pop()
                    val fourth = pop()
                    push(second)
                    push(first)
                    push(fourth)
                    push(third)
                    push(second)
                    push(first)
                  case (DUP2_X2, 2::1::1::_) =>
                    val first = pop()
                    val second = pop()
                    val third = pop()
                    push(first)
                    push(third)
                    push(second)
                    push(first)
                  case (opcode, _) =>
                    throw new UnsupportedOpcodeException(opcode, s"Stack's data type categories " +
                      s"(${stackCategories}) don't match the opcode's requirements.")
                }
              case DRETURN | FRETURN | IRETURN | LRETURN | ARETURN =>
                result = Some(pop())@
              case RETURN =>
                result = Some(VOID)
              case _ => throw new UnsupportedOpcodeException(opcode)
            }
          case node if isPseudo(node) => // Skip pseudo code
          case _ => throw new UnsupportedOpcodeException(opcode)
        }

        index += 1
      }
      if (result.isEmpty) {
        throw new ByteCodeParserException("Failed to parse the closure for unknown reason")
      }
      result.get
    }
    try {
      val result = invoke(applyMethod.instructions, 0, List.empty[Node], localVars)
      // As JVM treats Boolean, Byte, Short as Integer in runtime, we need to do a cast to change
      // the return type back to expected
      cast(result, getReturnType(applyMethod.desc))
    } finally {
      tracer.flush()
    }
  }
}
