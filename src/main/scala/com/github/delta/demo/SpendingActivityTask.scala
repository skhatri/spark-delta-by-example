package com.github.delta.demo

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{SaveMode, SparkSession}

object SpendingActivityTask extends App {

  val spark = SparkSession
    .builder
    .appName("spending-activity-task")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate()

  val eods = Seq("2021-03-02", "2021-03-03", "2021-03-05", "2021-03-08", "2021-03-09")

  eods.foreach(eodDate => {
    val isDelta = DeltaTable.isDeltaTable(R.outputPath)
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("timestamp", s"${eodDate}T13:00:00Z")
      .option("timestampAsOf", s"${eodDate}T13:00:00Z")
      .csv(s"${R.inputPath}/$eodDate/*.csv")
      .selectExpr("*", "false as deleted")

    if (isDelta) {
      val deltaTable = DeltaTable.forPath(R.outputPath)
      deltaTable.as("existing_data")
        .merge(df.as("new_data"), "existing_data.txn_id = new_data.txn_id")
        .whenMatched("new_data.deleted = true")
        .delete()
        .whenMatched()
        .updateExpr(Map("category" -> "new_data.category", "last_updated" -> "new_data.last_updated"))
        .whenNotMatched()
        .insertAll()
        .execute()
    } else {
      df.write
        .format("delta")
        .partitionBy(R.partitionKey)
        .mode(SaveMode.Overwrite)
        .save(R.outputPath)
    }
  })

  spark.stop()
  spark.close()
}
