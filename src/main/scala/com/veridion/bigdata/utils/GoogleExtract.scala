package com.veridion.bigdata.utils

import com.veridion.bigdata.model.GoogleDataset
import org.apache.spark.sql.{Dataset, SparkSession}

object GoogleExtract extends BaseExtract[GoogleDataset] {
  override val tableName: String = "GG_DF"
  override val delimeter: String = ","
  override val tableParamName: String = "google_dataset"

  override def extractFromSource(spark: SparkSession, input: String): Dataset[GoogleDataset] = {
    import spark.implicits._
    val sql =
      s"""
         SELECT
              address,
              category,
              city,
              country_code,
              country_name,
              name,
              phone,
              phone_country_code,
              raw_address,
              raw_phone,
              region_code,
              region_name,
              text,
              zip_code,
              domain
         FROM $tableName
      """

    spark.sql(sql)
      .as[GoogleDataset]
  }

}
