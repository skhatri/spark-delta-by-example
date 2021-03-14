package com.github.delta.demo

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object PrestoTrinoConfigTask extends App {

  val spark = SparkSession
    .builder
    .appName("presto-config-task")
    .master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate()

  val isDelta = DeltaTable.isDeltaTable(R.outputPath)
  if (isDelta) {
    val deltaTable = DeltaTable.forPath(R.outputPath)
    deltaTable.generate("symlink_format_manifest")
  }

  spark.stop()
  spark.close()
}
