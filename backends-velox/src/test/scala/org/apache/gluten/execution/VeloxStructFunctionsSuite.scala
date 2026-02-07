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
package org.apache.gluten.execution

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}

class VeloxStructFunctionsSuite extends VeloxWholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set(
        "spark.sql.optimizer.excludedRules",
        ConstantFolding.ruleName + "," +
          NullPropagation.ruleName)
  }

  test("struct with integer values") {
    runQueryAndCompare(
      "select struct(1, 2, 3) as s"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("struct with mixed types") {
    runQueryAndCompare(
      "select struct(1, 'hello', 3.14, true) as s"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("struct with column references") {
    runQueryAndCompare(
      "select struct(l_orderkey, l_partkey, l_suppkey) as s from lineitem limit 10"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("struct with null values") {
    runQueryAndCompare(
      "select struct(1, null, 'test') as s"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("nested struct") {
    runQueryAndCompare(
      "select struct(1, struct(2, 3)) as s"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("struct field access") {
    runQueryAndCompare(
      "select struct(1 as a, 2 as b).a as field_a"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("named_struct function") {
    runQueryAndCompare(
      "select named_struct('a', 1, 'b', 2, 'c', 3) as s"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("named_struct with column values") {
    runQueryAndCompare(
      "select named_struct('orderkey', l_orderkey, 'partkey', l_partkey) as s from lineitem limit 10"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("struct in filter condition") {
    runQueryAndCompare(
      "select l_orderkey from lineitem where struct(l_orderkey, l_partkey).col1 > 100 limit 10"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("struct with expressions") {
    runQueryAndCompare(
      "select struct(l_orderkey + 1, l_quantity * 2) as s from lineitem limit 10"
    )(checkGlutenPlan[ProjectExecTransformer])
  }
}
