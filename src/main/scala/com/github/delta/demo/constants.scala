package com.github.delta.demo

object R {
  val outputPath = Option(System.getenv("BASE_PATH")).getOrElse("/opt/data/activity")
  val inputPath = "src/main/resources/input"
  val partitionKey = "version"
}
