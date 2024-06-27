package com.scylladb.migrator.alternator

import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.scylladb.migrator.AttributeValueUtils.mapV1AttributeValueToV2
import com.scylladb.migrator.{DynamoUtils, readers, writers}
import com.scylladb.migrator.config.{Rename, SourceSettings, TargetSettings}
import com.scylladb.migrator.writers.DynamoStreamReplication
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.NonFatal
import scala.collection.JavaConverters._

object AlternatorMigrator {
  private val log = LogManager.getLogger("com.scylladb.migrator.alternator")

  def migrateFromDynamoDB(source: SourceSettings.DynamoDB,
                          target: TargetSettings.DynamoDB,
                          renames: List[Rename])(implicit spark: SparkSession): Unit = {
    val (sourceRDD, sourceTableDesc) = readers.DynamoDB.readRDD(spark, source)
    val maybeStreamedSource = if (target.streamChanges) Some(source) else None
    migrate(sourceRDD, sourceTableDesc, maybeStreamedSource, target, renames)
  }

  def migrateFromS3Export(source: SourceSettings.DynamoDBS3Export,
                          target: TargetSettings.DynamoDB,
                          renames: List[Rename])(implicit spark: SparkSession): Unit = {
    val (sourceRDD, sourceTableDesc) = readers.DynamoDBS3Export.readRDD(source)(spark.sparkContext)
    // Adapt the decoded items to the format expected by the EMR Hadoop connector
    val normalizedRDD =
      sourceRDD.map { item =>
        (new Text(), new DynamoDBItemWritable(item.transform((_, v) => mapV1AttributeValueToV2(v)).asJava))
      }
    if (target.streamChanges) {
      log.warn("'streamChanges: true' is not supported when the source is a DynamoDB S3 export.")
    }
    migrate(normalizedRDD, sourceTableDesc, None, target, renames)
  }

  /**
    * @param sourceRDD           Data to migrate
    * @param sourceTableDesc     Description of the table to replicate on the target database
    * @param maybeStreamedSource Settings of the source table in case `streamChanges` was `true`
    * @param target              Target table settings
    * @param renames             Renames
    * @param spark               Spark session
    */
  def migrate(sourceRDD: RDD[(Text, DynamoDBItemWritable)],
              sourceTableDesc: TableDescription,
              maybeStreamedSource: Option[SourceSettings.DynamoDB],
              target: TargetSettings.DynamoDB,
              renames: List[Rename])(implicit spark: SparkSession): Unit = {

    log.info("We need to transfer: " + sourceRDD.getNumPartitions + " partitions in total")

    log.info("Starting write...")

    try {
      val targetTableDesc = {
        for (streamedSource <- maybeStreamedSource) {
          log.info(
            "Source is a Dynamo table and change streaming requested; enabling Dynamo Stream")
          DynamoUtils.enableDynamoStream(streamedSource)
        }

        DynamoUtils.replicateTableDefinition(
          sourceTableDesc,
          target
        )
      }

      if (target.streamChanges && target.skipInitialSnapshotTransfer.contains(true)) {
        log.info("Skip transferring table snapshot")
      } else {
        writers.DynamoDB.writeRDD(target, renames, sourceRDD, targetTableDesc)
        log.info("Done transferring table snapshot")
      }

      for (streamedSource <- maybeStreamedSource) {
        log.info("Starting to transfer changes")
        val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

        DynamoStreamReplication.createDStream(
          spark,
          streamingContext,
          streamedSource,
          target,
          targetTableDesc,
          renames)

        streamingContext.start()
        streamingContext.awaitTermination()
      }
    } catch {
      case NonFatal(e) =>
        log.error("Caught error while writing the RDD.", e)
    }

  }

}
