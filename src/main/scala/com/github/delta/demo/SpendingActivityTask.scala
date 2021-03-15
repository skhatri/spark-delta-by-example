package com.github.delta.demo

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{SaveMode, SparkSession}

object SpendingActivityTask extends App {

  val sparkBuilder = SparkSession
    .builder
    .appName("spending-activity-task")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

  val spark = if (!Option(System.getenv("AWS_ACCESS_KEY_ID")).isEmpty) {
    sparkBuilder
      .config("spark.hadoop.fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY_ID"))
      .config("spark.hadoop.fs.s3a.secret.key", System.getenv("AWS_SECRET_ACCESS_KEY"))
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
      .getOrCreate()
  } else {
    sparkBuilder.getOrCreate()
  }

  val eods = Seq("2021-03-02", "2021-03-03", "2021-03-05", "2021-03-08", "2021-03-09")

  eods.foreach(eodDate => {
    val isDelta = DeltaTable.isDeltaTable(R.outputPath)
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("timestamp", s"${eodDate}T13:00:00Z")
      .option("timestampAsOf", s"${eodDate}T13:00:00Z")
      .csv(s"${R.inputPath}/$eodDate/*.csv")
      .selectExpr(
        "*",
        "false as deleted",
        "to_date('%s') as %s".format(eodDate, R.partitionKey),
        "to_date(txn_date) as transaction_date",
        "float(amount) as amt"
      ).drop("txn_date", "amount")
      .withColumnRenamed("amt", "amount")
      .withColumnRenamed("transaction_date", "txn_date")
      .repartition(2)


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

  DeltaTable.forPath(R.outputPath).generate("symlink_format_manifest")
  DeltaLogSnapshot.generateManifest(R.outputPath, "_delta_manifest")

  spark.stop()
  spark.close()
}
