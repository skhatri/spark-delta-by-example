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

  import spark.sqlContext._

  val isDelta = DeltaTable.isDeltaTable(R.outputPath)
  if (isDelta) {
    val deltaTable = DeltaTable.forPath(R.outputPath)
    deltaTable.toDF.printSchema()
    val availableVersions = deltaTable.history().map(row => row.getAs[Long]("version")).collect().sorted

    availableVersions.foreach(snapshotVersion => {
      val df = spark.read.format("delta")
        .option("versionAsOf", snapshotVersion)
        .load(R.outputPath)
      println(s"""task="total_activity_count", version=$snapshotVersion, total_rows=${df.count()}""")
    })

    //Find total number of activities by account
    val totalNumberOfActivitiesByAccount = deltaTable.toDF.groupBy("account").count()
    totalNumberOfActivitiesByAccount.show(false)

    //As of version 3, what was transaction id ```txn10``` labelled as?
    val transaction10LabelForVersion = spark.read.format("delta")
      .option("versionAsOf", 2).load(R.outputPath)
      .where("txn_id='txn10'")
    transaction10LabelForVersion.show(false)

    //What is the latest label of transaction id ```txn10``` and when was it last updated?
    val latestTransactionLabel = deltaTable.toDF
      .where("txn_id='txn10'")
    latestTransactionLabel.show(false)

    //Acc5 bought something from Apple Store Sydney on 2021-03-05, how did the label for this transaction change over time?
    availableVersions.foreach(snapshotVersion => {
      val df = spark.read.format("delta")
        .option("versionAsOf", snapshotVersion)
        .load(R.outputPath)
      val data = df.where("account='acc5' and txn_date='2021-03-05' and merchant='Apple Store Sydney'")
        .selectExpr("*", s"$snapshotVersion as snapshot_version")
      if (!data.isEmpty) {
        data.show(false)
      }
    })

  }

  spark.stop()
  spark.close()
}
