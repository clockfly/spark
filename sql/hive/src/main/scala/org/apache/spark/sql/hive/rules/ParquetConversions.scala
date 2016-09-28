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

package org.apache.spark.sql.hive.rules

import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetOptions}
import org.apache.spark.sql.hive.{HiveSessionState, MetastoreRelation}

/**
 * When scanning or writing to non-partitioned Metastore Parquet tables, convert them to Parquet
 * data source relations for better performance.
 */
class ParquetConversions(sessionState: HiveSessionState) extends Rule[LogicalPlan] {
  private def shouldConvertMetastoreParquet(relation: MetastoreRelation): Boolean = {
    relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") &&
      sessionState.convertMetastoreParquet
  }

  private def convertToParquetRelation(relation: MetastoreRelation): LogicalRelation = {
    val defaultSource = new ParquetFileFormat()
    val fileFormatClass = classOf[ParquetFileFormat]

    val mergeSchema = sessionState.convertMetastoreParquetWithSchemaMerging
    val options = Map(ParquetOptions.MERGE_SCHEMA -> mergeSchema.toString)

    Util.convertToLogicalRelation(relation, options, defaultSource, fileFormatClass, "parquet")
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.resolved || plan.analyzed) {
      return plan
    }

    plan transformUp {
      // Write path
      case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
        // Inserting into partitioned table is not supported in Parquet data source (yet).
        if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreParquet(r) =>
        InsertIntoTable(convertToParquetRelation(r), partition, child, overwrite, ifNotExists)

      // Read path
      case relation: MetastoreRelation if shouldConvertMetastoreParquet(relation) =>
        val parquetRelation = convertToParquetRelation(relation)
        SubqueryAlias(relation.tableName, parquetRelation, None)
    }
  }
}

