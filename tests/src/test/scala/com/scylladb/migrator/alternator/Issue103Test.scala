package com.scylladb.migrator.alternator

import com.scylladb.migrator.AttributeValueUtils.{mapValue, numericalValue, stringValue}
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration

import scala.collection.JavaConverters._
import scala.util.chaining._

// Reproduction of https://github.com/scylladb/scylla-migrator/issues/103
class Issue103Test extends MigratorSuite {

  withTable("Issue103Items").test("Issue #103 is fixed".ignore) { tableName =>
    // Insert two items
    val keys1 = Map("id" -> stringValue("4"))
    val attrs1 = Map(
      "AlbumTitle" -> stringValue("aaaaa"),
      "Awards"     -> numericalValue("4")
    )
    val item1Data = keys1 ++ attrs1

    val keys2 = Map("id" -> stringValue("999"))
    val attrs2 = Map(
      "asdfg" -> mapValue(
        "fffff" -> stringValue("asdfasdfs")
      )
    )
    val item2Data = keys2 ++ attrs2

    sourceDDb.putItem(tableName, item1Data.asJava)
    sourceDDb.putItem(tableName, item2Data.asJava)

    // Perform the migration
    successfullyPerformMigration("dynamodb-to-alternator-issue-103.yaml")

    // Check that both items have been correctly migrated to the target table
    checkItemWasMigrated(tableName, keys1, item1Data)
    checkItemWasMigrated(tableName, keys2, item2Data)
  }

}
