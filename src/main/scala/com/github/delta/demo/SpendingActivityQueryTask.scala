package com.github.delta.demo

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object SpendingActivityQueryTask extends App {

  val spark = SparkSession
    .builder
    .appName("spending-activity-query-task")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate()

  import spark.implicits._


  val isDelta = DeltaTable.isDeltaTable(R.outputPath)
  if (isDelta) {
    val deltaTable = DeltaTable.forPath(R.outputPath)
    val availableVersions = deltaTable.history().map(row => row.getAs[Long]("version")).collect()

    availableVersions.foreach(snapshotVersion => {
      val df = spark.read.format("delta")
        .option("versionAsOf", snapshotVersion)
        .load(R.outputPath)
      println(s"""task="total_activity_count", version=$snapshotVersion, total_rows=${df.count()}""")
    })
  }


  spark.stop()
  spark.close()
}
