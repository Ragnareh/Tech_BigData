package com.veridion.bigdata.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait BaseExtract[T] extends Logging {
  val tableName: String
  val delimeter: String
  val tableParamName: String

  def extract(spark: SparkSession, input: String): DataFrame = {
    logInfo("Extracting " + tableName + " from the source...")
    val data = extractFromSource(spark, input)
    logInfo(tableName + " extract finished")
    data.toDF()
  }

  def extractFromSource(spark: SparkSession, input: String): Dataset[T]

  def loadTable(spark: SparkSession, input: String): Unit = {
    logInfo("Loading " + tableName)
    spark
      .read
      .option("header", "true")
      .option("delimiter", delimeter)
      .csv(input)
      .createOrReplaceTempView(tableName)
    logInfo(tableName + " table loaded")
  }
}
