package com.github.delta.demo

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.util.SerializableConfiguration

object DeltaLogSnapshot {

  def generateManifest(path: String, targetBasePath: String): Unit = {
    val spark = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }
    val deltaLog = DeltaLog.forTable(spark, path)
    deltaLog.update(false)
    import spark.implicits._

    val versions = DeltaTable.forPath(path).history()
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

      val manifestPath = new Path(deltaLog.dataPath, s"$targetBasePath/$partitionFolder")
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

  }
}
