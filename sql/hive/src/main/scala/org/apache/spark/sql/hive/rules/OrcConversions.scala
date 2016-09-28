package org.apache.spark.sql.hive.rules
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.{HiveSessionState, MetastoreRelation}
import org.apache.spark.sql.hive.orc.OrcFileFormat

/**
 * When scanning Metastore ORC tables, convert them to ORC data source relations
 * for better performance.
 */
class OrcConversions(sessionState: HiveSessionState) extends Rule[LogicalPlan] {
  private def shouldConvertMetastoreOrc(relation: MetastoreRelation): Boolean = {
    relation.tableDesc.getSerdeClassName.toLowerCase.contains("orc") &&
      sessionState.convertMetastoreOrc
  }

  private def convertToOrcRelation(relation: MetastoreRelation): LogicalRelation = {
    val defaultSource = new OrcFileFormat()
    val fileFormatClass = classOf[OrcFileFormat]
    val options = Map[String, String]()

    Util.convertToLogicalRelation(relation, options, defaultSource, fileFormatClass, "orc")
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.resolved || plan.analyzed) {
      return plan
    }

    plan transformUp {
      // Write path
      case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
        // Inserting into partitioned table is not supported in Orc data source (yet).
        if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreOrc(r) =>
        InsertIntoTable(convertToOrcRelation(r), partition, child, overwrite, ifNotExists)

      // Read path
      case relation: MetastoreRelation if shouldConvertMetastoreOrc(relation) =>
        val orcRelation = convertToOrcRelation(relation)
        SubqueryAlias(relation.tableName, orcRelation, None)
    }
  }
}