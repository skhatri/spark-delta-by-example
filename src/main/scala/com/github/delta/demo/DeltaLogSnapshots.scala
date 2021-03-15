package com.github.delta.demo

import org.apache.spark.sql.SparkSession

object DeltaLogSnapshots extends App {

  val spark = SparkSession
    .builder
    .appName("delta-log-snapshots")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.hadoop.fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", System.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .getOrCreate()

  DeltaLogSnapshot.generateManifest(R.outputPath, "_delta_manifest")


  spark.stop()
  spark.close()
}
