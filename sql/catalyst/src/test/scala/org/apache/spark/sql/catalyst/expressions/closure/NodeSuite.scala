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

import org.apache.xbean.asm5.Type.{BOOLEAN_TYPE, INT_TYPE}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.closure.DSL._

class NodeSuite extends SparkFunSuite {
  test("treeString") {
    val foo = FunctionCall(
      obj = cast[java.lang.String](arg[AnyRef]),
      className = "java.lang.String",
      method = "equals",
      arguments = List(Constant("lisa")),
      dataType = BOOLEAN_TYPE)

    assert(foo.treeString ==
      """FunctionCall[Z](java.lang.String, equals)
        |  Cast[Ljava/lang/String;]
        |    Argument[Ljava/lang/Object;]
        |  Constant[Ljava/lang/String;](lisa)
        |""".stripMargin)
    assert(foo.toString == "java.lang.String.equals(Cast(Argument,Ljava/lang/String;), lisa)")

    val ifElse = If(gt(arg[Int], Constant(3)), arg[Int], Constant(3), INT_TYPE)
    assert(ifElse.treeString ==
      """If[I](Argument > 3)
        |  Argument[I]
        |  Constant[I](3)
        |""".stripMargin)

    assert(ifElse.toString == "If(Argument > 3,Argument,3,I)")
  }
}
