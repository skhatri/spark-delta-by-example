package com.github.delta.demo

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.util.SerializableConfiguration

object DeltaLogSnapshots extends App {

  val spark = SparkSession
    .builder
    .appName("delta-log-snapshots")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate()

  val deltaLog = DeltaLog.forTable(spark, R.outputPath)
  val currentSnapshot = deltaLog.snapshot

  val snapshot = deltaLog.update(false)
  val allSnapshotFiles = snapshot.allFiles.collect()
  import spark.implicits._

  val versions = DeltaTable.forPath(R.outputPath).history()
    .select("version")
    .map(line => line.getAs[Long]("version"))
    .collect().sorted


  versions.foreach(v => {
    val versionSnapshot = deltaLog.getSnapshotAt(v)
    val colName = versionSnapshot.metadata.partitionColumns.head
    val snapshotFiles = versionSnapshot.allFiles.collect()
    val maxValue = snapshotFiles.map(_.partitionValues.get(colName).get).max
    val partitionFolder = s"$colName=$maxValue"
    val hadoopConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())

    val manifestPath = new Path(deltaLog.dataPath, s"_delta_manifests/$partitionFolder")
    val fs = manifestPath.getFileSystem(hadoopConf.value)
    fs.mkdirs(manifestPath)

    val tableAbsPathForManifest =
      LogStore(spark.sparkContext).resolvePathOnPhysicalStorage(deltaLog.dataPath).toString
    val listOfFiles = snapshotFiles.map(a => {
      DeltaFileOperations.absolutePath(tableAbsPathForManifest, a.path).toString
    }).iterator
    val logStore = LogStore(SparkEnv.get.conf, hadoopConf.value)
    logStore.write(new Path(manifestPath, "manifest"), listOfFiles, overwrite = true)
  })

  spark.stop()
  spark.close()
}
