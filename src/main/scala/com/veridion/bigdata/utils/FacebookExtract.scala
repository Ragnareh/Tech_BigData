package com.veridion.bigdata.utils

import com.veridion.bigdata.model.FacebookDataset
import org.apache.spark.sql.{Dataset, SparkSession}

object FacebookExtract extends BaseExtract[FacebookDataset] {
  override val tableName: String = "FB_DF"
  override val delimeter: String = ","
  override val tableParamName: String = "facebook_dataset"

  override def extractFromSource(spark: SparkSession, input: String): Dataset[FacebookDataset] = {
    import spark.implicits._
    val sql =
      s"""
         SELECT
              domain,
              address,
              categories,
              city,
              country_code,
              country_name,
              description,
              email,
              link,
              name,
              page_type,
              phone,
              phone_country_code,
              region_code,
              region_name,
              zip_code
         FROM $tableName
      """

    spark.sql(sql)
      .as[FacebookDataset]
  }

}
