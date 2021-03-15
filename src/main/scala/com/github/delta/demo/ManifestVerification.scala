package com.github.delta.demo

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

object ManifestVerification extends App {

  val spark = SparkSession
    .builder
    .appName("manifest-verification")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  val isDelta = DeltaTable.isDeltaTable(R.outputPath)
  if (isDelta) {
    val version1 = spark.read.textFile(R.outputPath + "/_symlink_format_manifest/version=2021-03-02/manifest").collect()
    val version3 = spark.read.textFile(R.outputPath + "/_symlink_format_manifest/version=2021-03-08/manifest").collect()

    version1.foreach(fileName => {
      println(s"File: $fileName")
      spark.read.parquet(fileName).show(false)
    })

  }

  val deltaLog = DeltaLog.forTable(spark, R.outputPath)
  deltaLog

  spark.stop()
  spark.close()
}
