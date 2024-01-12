package com.veridion.bigdata

import com.veridion.bigdata.utils.{FacebookExtract, FuzzyUtil, GoogleExtract, WebsiteExtract}
import org.apache.spark.sql.functions.{col, current_date, lit, lower}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object Logic {

  def run(spark: SparkSession, input: String, output: String): Unit = {
    spark
      .read
      .csv(input)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(output)

  }

  def readInput(spark: SparkSession, input: String, delimeter: String = ","): DataFrame = {
    spark
      .read
      .option("header", "true")
      .option("delimiter", delimeter)
      .csv(input)
  }

  def writeOutput(df: DataFrame, output: String, partitions: List[String], format: String = "parquet"): Unit = {
    if (format != "parquet") {
      if (partitions.isEmpty) {
        df.repartition(1)
          .write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv(output)
      }
      else {
        df.write
          .mode(SaveMode.Overwrite)
          .partitionBy(partitions: _*)
          .option("header", "true")
          .csv(output)
      }

    } else {
      if (partitions.isEmpty) {
        df.repartition(1)
          .write
          .mode(SaveMode.Overwrite)
          .parquet(output)
      }
      else {
        df.write
          .mode(SaveMode.Overwrite)
          .partitionBy(partitions: _*)
          .parquet(output)
      }
    }

  }

  def transformation(df: DataFrame): DataFrame = {
    df.withColumn("version", lit("0"))
      .withColumn("ingestion_date", lit(current_date()))
  }

  def runRemote(spark: SparkSession, input: String, output: String, delimeter: String = ","): Unit = {
    //run(spark, "s3://rawzone-batch/config.csv", "s3://rawzone-batch/am/config.csv")
    println("V1")
    //run(spark, input, output)
    println("Step 1: read the file")
    val readDf = readInput(spark, input, delimeter)
    println("Step 2: transform the file")
    val transDf = transformation(readDf)
    println("Step 3: transform the file")

    val fileName = input.split("\\/").last
    val tableName = fileName.split("\\.").head
    println(s"Step 3: ${tableName}")
    readDf.createOrReplaceTempView(tableName)
    spark.sql(s"select * from ${tableName}").show()
    println("Step 4: write the file")
    writeOutput(transDf, output, List("version", "ingestion_date"))

  }

  def prepareDfs(spark: SparkSession, input: String): Unit = {
    FacebookExtract.loadTable(spark, input + "/" + FacebookExtract.tableParamName + ".csv")
    GoogleExtract.loadTable(spark, input + "/" + GoogleExtract.tableParamName + ".csv")
    WebsiteExtract.loadTable(spark, input + "/" + WebsiteExtract.tableParamName + ".csv")

    //FacebookExtract.extract(spark, input + "/" + FacebookExtract.tableParamName + ".csv")
    //GoogleExtract.extract(spark, input + "/" + GoogleExtract.tableParamName + ".csv")
    //WebsiteExtract.extract(spark, input + "/" + WebsiteExtract.tableParamName + ".csv")
  }

  def makeDomainsDf(spark: SparkSession): DataFrame = {

    val domainDf = spark.sql(
      s"""
         |select distinct fb.domain as fb_domain, fb.country_code as fb_country_code, gg.domain as gg_domain, gg.country_code as gg_country_code, wb.root_domain as wb_root_domain,
         |coalesce(fb.domain, gg.domain, wb.root_domain) as domain,
         |coalesce(fb.country_code, gg.country_code) as country_code,
         |coalesce(fb.country_name, gg.country_name, wb.main_country) as country,
         |coalesce(fb.city, gg.city, wb.main_city) as city
         |from ${GoogleExtract.tableName} gg
         |left join ${FacebookExtract.tableName} fb on (fb.domain = gg.domain and fb.country_code = gg.country_code)
         |left join ${WebsiteExtract.tableName} wb on (fb.domain = wb.root_domain and fb.country_name = wb.main_country)
         |""".stripMargin) //.show(100)
    domainDf.createOrReplaceTempView("DOMAINS_DF")
    domainDf
  }

  def makeCompaniesDf(spark: SparkSession): DataFrame = {
    val gg_companies = spark.sql(
      s"""
        |select gg.domain, gg.country_name, gg.city, gg.name
        |from ${GoogleExtract.tableName} gg
        |where gg.name is not null
        |order by gg.domain, gg.country_name, gg.city, gg.name
        |""".stripMargin)

    val fb_companies = spark.sql(
      s"""
        |select fb.domain, fb.country_name, fb.city, fb.name
        |from ${FacebookExtract.tableName} fb
        |where fb.name is not null
        |order by fb.domain, fb.country_name, fb.city, fb.name
        |""".stripMargin)

    val wb_companies = spark.sql(
      s"""
        |select wb.root_domain as domain, wb.main_country as country_name, wb.main_city as city, wb.legal_name as name
        |from ${WebsiteExtract.tableName} wb
        |where wb.legal_name is not null
        |order by wb.root_domain, wb.main_country, wb.main_city, wb.legal_name
        |""".stripMargin)

    val gg_fb = gg_companies.except(fb_companies)
    val fb_gg = fb_companies.except(gg_companies)
    val gg_wb = gg_companies.except(wb_companies)
    val wb_gg = wb_companies.except(gg_companies)
    val fb_wb = fb_companies.except(wb_companies)
    val wb_fb = wb_companies.except(fb_companies)
    println(s"gg_companies ${gg_companies.count()}")
    println(s"fb_companies ${fb_companies.count()}")
    println(s"wb_companies ${wb_companies.count()}")
    println(s"gg_companies except fb_companies ${gg_fb.count()}")
    println(s"fb_companies except gg_companies ${fb_gg.count()}")
    println(s"gg_companies except wb_companies ${gg_wb.count()}")
    println(s"wb_companies except gg_companies ${wb_gg.count()}")
    println(s"fb_companies except wb_companies ${fb_wb.count()}")
    println(s"wb_companies except fb_companies ${wb_fb.count()}")

    val companiesDf = gg_companies.union(fb_companies).union(wb_companies)
    println(s"all companies with names ${companiesDf.count()}")

    companiesDf.createOrReplaceTempView("COMPANIES_DF")
    companiesDf
  }

  def makeRegionsDf(spark: SparkSession): DataFrame = {
    val regionsDf = spark.sql(
      s"""SELECT region_code, region_name
         |FROM (
         |SELECT fb.region_code as region_code, fb.region_name as region_name
         |  FROM ${FacebookExtract.tableName} fb
         |UNION ALL
         |SELECT gg.region_code as region_code, gg.region_name as region_name
         |  FROM ${GoogleExtract.tableName} gg
         |UNION ALL
         |SELECT NULL as region_code, wb.main_region as region_name /*think how to enrich region_code here*/
         |  FROM ${WebsiteExtract.tableName} wb
         |) WHERE region_code IS NOT NULL AND region_name IS NOT NULL
         |GROUP BY region_code, region_name
         |ORDER BY region_name, region_code
         |""".stripMargin)
    regionsDf
  }

  def makeCountriesDf(spark: SparkSession): DataFrame = {
    val countriesDf = spark.sql(
      s"""SELECT country_code, country_name, city
         |FROM (
         |SELECT fb.country_code as country_code, fb.country_name as country_name, fb.city as city
         |  FROM ${FacebookExtract.tableName} fb
         |UNION ALL
         |SELECT gg.country_code as country_code, gg.country_name as country_name, gg.city as city
         |  FROM ${GoogleExtract.tableName} gg
         |UNION ALL
         |SELECT NULL as country_code, wb.main_country as country_name, wb.main_city as city /*think how to enrich country_code here*/
         |  FROM ${WebsiteExtract.tableName} wb
         |)
         |GROUP BY country_code, country_name, city
         |ORDER BY country_name, country_code, city
         |""".stripMargin)
    countriesDf
  }

  def makeAdressesDf(spark: SparkSession): DataFrame = {
    val adressesInitDf = spark.sql(
      s"""
         |SELECT COALESCE(gg.domain, fb.domain, wb.root_domain) AS domain,
         |       COALESCE(gg.country_name, fb.country_name, wb.main_country) AS country_name,
         |       COALESCE(gg.country_code, fb.country_code) AS country_code,
         |       COALESCE(gg.city, fb.city, wb.main_city) AS city,
         |       fb.address AS fb_address,
         |       gg.address AS gg_address,
         |       fb.region_name AS fb_region_name,
         |       fb.country_code AS fb_country_code,
         |       fb.country_name AS fb_country_name,
         |       fb.city AS fb_city,
         |       gg.region_name AS gg_region_name,
         |       gg.country_code AS gg_country_code,
         |       gg.country_name AS gg_country_name,
         |       gg.city AS gg_city,
         |       wb.main_region AS wb_main_region,
         |       wb.main_country AS wb_main_country,
         |       wb.main_city AS wb_city,
         |       fb.categories AS fb_categories,
         |       gg.category AS gg_category,
         |       wb.s_category AS wb_category,
         |       fb.name AS fb_name,
         |       fb.phone AS fb_phone,
         |       fb.region_code,
         |       fb.zip_code,
         |       gg.name AS gg_name,
         |       gg.phone AS gg_phone,
         |       gg.phone_country_code,
         |       wb.legal_name AS wb_name,
         |       wb.phone AS wb_phone
         |FROM ${GoogleExtract.tableName} gg
         |FULL OUTER JOIN ${FacebookExtract.tableName} fb ON (gg.domain = fb.domain AND gg.country_code = fb.country_code AND gg.country_name = fb.country_name AND gg.city = fb.city)
         |FULL OUTER JOIN ${WebsiteExtract.tableName} wb ON (gg.domain = wb.root_domain AND gg.country_name = wb.main_country AND gg.city = wb.main_city)
         |WHERE (gg.name IS NOT NULL OR fb.name IS NOT NULL OR wb.legal_name IS NOT NULL)
         |
         |""".stripMargin)

    adressesInitDf.createOrReplaceTempView("INIT_ADDR_DF")

    val adressesDf = spark.sql(
      """
        |SELECT domain,
        |       collisionPicker(coalesce(fb_region_name, ""), coalesce(gg_region_name, ""), gg_fb_region_ratio) AS region_name,
        |       coalesce(gg_country_name, fb_country_name, wb_main_country) AS country_name,
        |       coalesce(gg_city, fb_city, wb_city) AS city,
        |       collisionPicker(coalesce(gg_address, ""), coalesce(fb_address, ""), gg_fb_address_ratio) AS address,
        |       collisionPicker(collisionPicker(coalesce(gg_phone, ""), coalesce(fb_phone, ""), gg_fb_phone_ratio), wb_phone, gg_wb_phone_ratio) AS phone,
        |       coalesce(gg_name, fb_name, wb_name) AS company_name,
        |       /*array_join(collect_list(fb_category, gg_category, wb_category),'/') AS list_src AS category,*/
        |       collisionPicker(collisionPicker(coalesce(gg_category, ""), coalesce(fb_categories, ""), gg_fb_category_ratio), wb_category, gg_wb_category_ratio) AS category
        |FROM
        |(
        |SELECT *,
        |       fuzzySortedPartialRatio(coalesce(gg_address, ""), coalesce(fb_address, "")) AS gg_fb_address_ratio,
        |       fuzzySortedPartialRatio(coalesce(gg_region_name, ""), coalesce(fb_region_name, "")) AS gg_fb_region_ratio,
        |       fuzzySortedPartialRatio(coalesce(gg_region_name, ""), coalesce(wb_main_region, "")) AS gg_wb_region_ratio,
        |       fuzzySortedPartialRatio(coalesce(gg_category, ""), coalesce(fb_categories, "")) AS gg_fb_category_ratio,
        |       fuzzySortedPartialRatio(coalesce(gg_category, ""), coalesce(wb_category, "")) AS gg_wb_category_ratio,
        |       fuzzySortedPartialRatio(coalesce(gg_phone, ""), coalesce(fb_phone, "")) AS gg_fb_phone_ratio,
        |       fuzzySortedPartialRatio(coalesce(gg_phone, ""), coalesce(wb_phone, "")) AS gg_wb_phone_ratio
        |FROM INIT_ADDR_DF
        |)
        |
        |""".stripMargin)

    adressesDf.createOrReplaceTempView("ADDR_DF")
    adressesDf.distinct()
  }

  def checkDfs(spark: SparkSession): Unit = {
    val companiesDf = makeCompaniesDf(spark).select("domain","country_name", "city", "name").withColumn("name", lower(col("name")))
    val adressesDf = spark.sql("select domain, country_name, city, lower(company_name) as name from ADDR_DF")
    val cnt1 = companiesDf.except(adressesDf).count()
    val cnt2 = adressesDf.except(companiesDf).count()
    println(s"cnt1: ${cnt1}; cnt2: ${cnt2}")
    //cnt1: 31897; cnt2: 0
    //companiesDf.except(adressesDf).orderBy("domain", "name","country_name", "city").show(100)
    val recheck = spark.sql(
      s"""SELECT *
         |FROM
         |  (SELECT fuzzySortedPartialRatio(coalesce(a.company_name, ""), coalesce(c.name, ""))  AS TEMP
         |   FROM COMPANIES_DF c
         |   LEFT JOIN ADDR_DF a ON (a.domain = c.domain
         |                           AND a.company_name = c.name
         |                           AND a.city = c.city))
         |WHERE TEMP <= 25""".stripMargin).count()
    //fuzzy cnt: 110617
    println(s"fuzzy cnt: ${recheck}")

  }

  def runLogic(spark: SparkSession, input: String, output: String): Unit = {
    FuzzyUtil.init(spark)
    println("Step 1: read the files")
    prepareDfs(spark, input)
    println("Step 2: transformations")
    spark.sql(s"select * from ${GoogleExtract.tableName}").show(10)
    spark.sql(s"select * from ${FacebookExtract.tableName}").show(10)
    spark.sql(s"select * from ${WebsiteExtract.tableName}").show(10)
    val domainsDf = makeDomainsDf(spark)
    val adressesDf = makeAdressesDf(spark)
    println("Step 3: join checks")
    checkDfs(spark)
    println("Step 4: write the outputs")
    writeOutput(domainsDf, output + "/domainsV3", List(), format="csv")
    writeOutput(adressesDf, output + "/adressesV3", List(), format="csv")
  }

}
