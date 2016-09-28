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

package org.apache.spark.sql.hive

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.exec.{UDAF, UDF}
import org.apache.hadoop.hive.ql.exec.{FunctionRegistry => HiveFunctionRegistry}
import org.apache.hadoop.hive.ql.udf.generic.{AbstractGenericUDAFResolver, GenericUDF, GenericUDTF}

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTableType, FunctionResourceLoader, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat, HadoopFsRelation, LogicalRelation, PartitionDirectory, PartitionSpec}
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, DoubleType, StructType}
import org.apache.spark.util.Utils


// TODO: Merge HiveSessionCatalog with SessionCatalog...
private[sql] class HiveSessionCatalog(
    externalCatalog: HiveExternalCatalog,
    sparkSession: SparkSession,
    functionResourceLoader: FunctionResourceLoader,
    functionRegistry: FunctionRegistry,
    conf: SQLConf,
    hadoopConf: Configuration)
  extends SessionCatalog(
    externalCatalog,
    functionResourceLoader,
    functionRegistry,
    conf,
    hadoopConf) {


  /** A cache of Spark SQL data source tables that have been accessed. */
  protected[hive] val cachedDataSourceTables: LoadingCache[QualifiedTableName, LogicalPlan] = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalPlan]() {
      override def load(in: QualifiedTableName): LogicalPlan = {
        logDebug(s"Creating new cached data source for $in")
        val table = sparkSession.sharedState.externalCatalog.getTable(in.database, in.name)

        val dataSource =
          DataSource(
            sparkSession,
            userSpecifiedSchema = Some(table.schema),
            partitionColumns = table.partitionColumnNames,
            bucketSpec = table.bucketSpec,
            className = table.provider.get,
            options = table.storage.properties)

        LogicalRelation(
          dataSource.resolveRelation(),
          catalogTable = Some(table))
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  override def lookupRelation(name: TableIdentifier, alias: Option[String]): LogicalPlan = {
    val table = formatTableName(name.table)
    if (name.database.isDefined || !tempTables.contains(table)) {
      val database = name.database.map(formatDatabaseName)
      val newName = name.copy(database = database, table = table)
      lookupRelationImpl(newName, alias)
    } else {
      val relation = tempTables(table)
      val tableWithQualifiers = SubqueryAlias(table, relation, None)
      // If an alias was specified by the lookup, wrap the plan in a subquery so that
      // attributes are properly qualified with this alias.
      alias.map(a => SubqueryAlias(a, tableWithQualifiers, None)).getOrElse(tableWithQualifiers)
    }
  }

  def lookupRelationImpl(
    tableIdent: TableIdentifier,
    alias: Option[String]): LogicalPlan = {
    val qualifiedTableName = getQualifiedTableName(tableIdent)
    val table = sparkSession.sharedState.externalCatalog.getTable(
      qualifiedTableName.database, qualifiedTableName.name)

    if (DDLUtils.isDatasourceTable(table)) {
      val dataSourceTable = cachedDataSourceTables(qualifiedTableName)
      val qualifiedTable = SubqueryAlias(qualifiedTableName.name, dataSourceTable, None)
      // Then, if alias is specified, wrap the table with a Subquery using the alias.
      // Otherwise, wrap the table with a Subquery using the table name.
      alias.map(a => SubqueryAlias(a, qualifiedTable, None)).getOrElse(qualifiedTable)
    } else if (table.tableType == CatalogTableType.VIEW) {
      val viewText = table.viewText.getOrElse(sys.error("Invalid view without text."))
      SubqueryAlias(
        alias.getOrElse(table.identifier.table),
        sparkSession.sessionState.sqlParser.parsePlan(viewText),
        Option(table.identifier))
    } else {
      val qualifiedTable =
        MetastoreRelation(
          qualifiedTableName.database, qualifiedTableName.name)(table, sparkSession)
      alias.map(a => SubqueryAlias(a, qualifiedTable, None)).getOrElse(qualifiedTable)
    }
  }

  // ----------------------------------------------------------------
  // | Methods and fields for interacting with HiveMetastoreCatalog |
  // ----------------------------------------------------------------


  override def refreshTable(name: TableIdentifier): Unit = {
    super.refreshTable(name)
    cachedDataSourceTables.invalidate(getQualifiedTableName(name))
  }

  def invalidateCache(): Unit = {
    cachedDataSourceTables.invalidateAll()
  }

  private def getCached(
    tableIdentifier: QualifiedTableName,
    pathsInMetastore: Seq[String],
    metastoreRelation: MetastoreRelation,
    schemaInMetastore: StructType,
    expectedFileFormat: Class[_ <: FileFormat],
    expectedBucketSpec: Option[BucketSpec],
    partitionSpecInMetastore: Option[PartitionSpec]): Option[LogicalRelation] = {

    cachedDataSourceTables.getIfPresent(tableIdentifier) match {
      case null => None // Cache miss
      case logical @ LogicalRelation(relation: HadoopFsRelation, _, _) =>
        val cachedRelationFileFormatClass = relation.fileFormat.getClass

        expectedFileFormat match {
          case `cachedRelationFileFormatClass` =>
            // If we have the same paths, same schema, and same partition spec,
            // we will use the cached relation.
            val useCached =
              relation.location.paths.map(_.toString).toSet == pathsInMetastore.toSet &&
                logical.schema.sameType(schemaInMetastore) &&
                relation.bucketSpec == expectedBucketSpec &&
                relation.partitionSpec == partitionSpecInMetastore.getOrElse {
                  PartitionSpec(StructType(Nil), Array.empty[PartitionDirectory])
                }

            if (useCached) {
              Some(logical)
            } else {
              // If the cached relation is not updated, we invalidate it right away.
              cachedDataSourceTables.invalidate(tableIdentifier)
              None
            }
          case _ =>
            logWarning(
              s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} " +
                s"should be stored as $expectedFileFormat. However, we are getting " +
                s"a ${relation.fileFormat} from the metastore cache. This cached " +
                s"entry will be invalidated.")
            cachedDataSourceTables.invalidate(tableIdentifier)
            None
        }
      case other =>
        logWarning(
          s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} should be stored " +
            s"as $expectedFileFormat. However, we are getting a $other from the metastore cache. " +
            s"This cached entry will be invalidated.")
        cachedDataSourceTables.invalidate(tableIdentifier)
        None
    }
  }

  def getQualifiedTableName(tableIdent: TableIdentifier): QualifiedTableName = {
    QualifiedTableName(
      tableIdent.database.getOrElse(getCurrentDatabase).toLowerCase,
      tableIdent.table.toLowerCase)
  }
}
