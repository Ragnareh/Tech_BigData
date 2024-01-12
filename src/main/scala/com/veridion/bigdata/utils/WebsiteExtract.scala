package com.veridion.bigdata.utils

import com.veridion.bigdata.model.WebsiteDataset
import org.apache.spark.sql.{Dataset, SparkSession}

object WebsiteExtract extends BaseExtract[WebsiteDataset] {
  override val tableName: String = "WB_DF"
  override val delimeter: String = ";"
  override val tableParamName: String = "website_dataset"

  override def extractFromSource(spark: SparkSession, input: String): Dataset[WebsiteDataset] = {
    import spark.implicits._
    val sql =
      s"""
         SELECT
              root_domain,
              domain_suffix,
              language,
              legal_name,
              main_city,
              main_country,
              main_region,
              phone,
              site_name,
              tld,
              s_category
         FROM $tableName
      """

    spark.sql(sql)
      .as[WebsiteDataset]
  }

}
