package com.veridion.bigdata

//import com.veridion.bigdata.Logic.writeOutput
//import com.veridion.bigdata.utils.FuzzyUtil
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LogicTest extends AnyFunSuite with BeforeAndAfterAll with Matchers {

  val runningPath: String = getClass.getClassLoader.getResource(".").getPath
  val testPath: String = s"$runningPath/../../src/test/resources"
  /*
  test("should work") {
    val spark = SparkSession.builder().appName("Test").master("local[5]").getOrCreate()
    val input = s"$testPath/test.csv"
    val output = s"$runningPath/../../target/testoutput"
    Logic.run(spark, input, output)
  }
  */
  /*
  test("Playground: local parses") {
    val spark = SparkSession.builder().appName("Test").master("local[5]")
      .config("spark.driver.extraClassPath", "/opt/")
      .getOrCreate()

    FuzzyUtil.init(spark)
    //spark.sql("""select fuzzySortedPartialRatio("one", "two") as gg_fb_phone_ratio""").show()

    Logic.runRemote(spark, s"$testPath/website_dataset.csv", s"$runningPath/../../target/testoutput/website", delimeter = ";")

    Logic.runRemote(spark, s"$testPath/google_dataset.csv", s"$runningPath/../../target/testoutput/google")

    Logic.runRemote(spark, s"$testPath/facebook_dataset.csv", s"$runningPath/../../target/testoutput/facebook")

    //domains
    val domainDf = spark.sql(
      """
        |select distinct fb.domain as fb_domain, fb.country_code as fb_country_code, gg.domain as gg_domain, gg.country_code as gg_country_code, wb.root_domain as wb_root_domain,
        |coalesce(fb.domain, gg.domain, wb.root_domain) as domain,
        |coalesce(fb.country_code, gg.country_code) as country_code,
        |coalesce(fb.country_name, gg.country_name, wb.main_country) as country,
        |coalesce(fb.city, gg.city, wb.main_city) as city
        |from google_dataset gg
        |left join facebook_dataset fb on (fb.domain = gg.domain and fb.country_code = gg.country_code)
        |left join website_dataset wb on (fb.domain = wb.root_domain and fb.country_name = wb.main_country)
        |""".stripMargin)//.show(100)

    writeOutput(domainDf, s"$runningPath/../../target/testoutput/domains", List(), format="csv")



    val gg_companies = spark.sql(
      """
        |select gg.domain, gg.country_name, gg.city, gg.name
        |from google_dataset gg
        |where gg.name is not null
        |order by gg.domain, gg.country_name, gg.city, gg.name
        |""".stripMargin)

    val fb_companies = spark.sql(
      """
        |select fb.domain, fb.country_name, fb.city, fb.name
        |from facebook_dataset fb
        |where fb.name is not null
        |order by fb.domain, fb.country_name, fb.city, fb.name
        |""".stripMargin)

    val wb_companies = spark.sql(
      """
        |select wb.root_domain as domain, wb.main_country as country_name, wb.main_city as city, wb.legal_name as name
        |from website_dataset wb
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
    writeOutput(companiesDf, s"$runningPath/../../target/testoutput/companies", List(), format = "csv")
/*
    gg_fb.show()
    fb_gg.show()
    gg_wb.show()
    wb_gg.show()
    fb_wb.show()
    wb_fb.show()
*/
    val regionsDf = spark.sql(
      s"""SELECT region_code, region_name
         |FROM (
         |SELECT fb.region_code as region_code, fb.region_name as region_name
         |  FROM facebook_dataset fb
         |UNION ALL
         |SELECT gg.region_code as region_code, gg.region_name as region_name
         |  FROM google_dataset gg
         |UNION ALL
         |SELECT NULL as region_code, wb.main_region as region_name /*think how to enrich region_code here*/
         |  FROM website_dataset wb
         |) WHERE region_code IS NOT NULL AND region_name IS NOT NULL
         |GROUP BY region_code, region_name
         |ORDER BY region_name, region_code
         |""".stripMargin)

    writeOutput(regionsDf, s"$runningPath/../../target/testoutput/regions", List(), format="csv")

    val countriesDf = spark.sql(
      s"""SELECT country_code, country_name, city
         |FROM (
         |SELECT fb.country_code as country_code, fb.country_name as country_name, fb.city as city
         |  FROM facebook_dataset fb
         |UNION ALL
         |SELECT gg.country_code as country_code, gg.country_name as country_name, gg.city as city
         |  FROM google_dataset gg
         |UNION ALL
         |SELECT NULL as country_code, wb.main_country as country_name, wb.main_city as city /*think how to enrich country_code here*/
         |  FROM website_dataset wb
         |)
         |GROUP BY country_code, country_name, city
         |ORDER BY country_name, country_code, city
         |""".stripMargin)
    countriesDf.show()

    writeOutput(countriesDf, s"$runningPath/../../target/testoutput/countries", List(), format="csv")

    //Addresses V1
    val adressesV1Df = spark.sql(
      """select coalesce(gg.domain, fb.domain, wb.root_domain) as domain, coalesce(gg.country_code, fb.country_code) as country_code,
        |fb.address as fb_address, gg.address as gg_address,
        |fuzzySortedPartialRatio(coalesce(gg.address,""), coalesce(fb.address,"")) as gg_fb_address_ratio,
        |fb.region_name as fb_region_name,fb.country_code as fb_country_code, fb.country_name as fb_country_name, fb.city as fb_city,
        |fb.region_code as fb_region_code,
        |gg.region_name as gg_region_name, gg.country_code as gg_country_code,gg.country_name as gg_country_name, gg.city as gg_city,
        |
        |fuzzySortedPartialRatio(coalesce(gg.region_name,""), coalesce(fb.region_name,"")) as gg_fb_region_ratio,
        |fuzzySortedPartialRatio(coalesce(gg.region_name,""), coalesce(wb.main_region,"")) as gg_wb_region_ratio,
        |wb.main_region as wb_main_region,wb.main_country as wb_main_country, wb.main_city as wb_city,
        |fb.categories as fb_categories, gg.category as gg_category, wb.s_category as wb_category,
        |fuzzySortedPartialRatio(coalesce(gg.category,""), coalesce(fb.categories,"")) as gg_fb_category_ratio,
        |fuzzySortedPartialRatio(coalesce(gg.category,""), coalesce(wb.s_category,"")) as gg_wb_category_ratio,
        |fb.name as fb_name, gg.name as gg_name, wb.legal_name as wb_name,
        |fb.phone as fb_phone, gg.phone as gg_phone, wb.phone as wb_phone,
        |fuzzySortedPartialRatio(coalesce(gg.phone,""), coalesce(fb.phone,"")) as gg_fb_phone_ratio,
        |fuzzySortedPartialRatio(coalesce(gg.phone,""), coalesce(wb.phone,"")) as gg_wb_phone_ratio
        |from google_dataset gg
        |left join facebook_dataset fb on (gg.domain = fb.domain AND gg.region_code = fb.region_code AND gg.country_code = fb.country_code AND gg.city = fb.city)
        |left join website_dataset wb on (gg.domain = wb.root_domain AND gg.region_name = wb.main_region AND gg.country_name = wb.main_country AND gg.city = wb.main_city)
        |order by coalesce(gg.domain, fb.domain, wb.root_domain), coalesce(gg.country_code, fb.country_code)
        |""".stripMargin) //.show(100)

    writeOutput(adressesV1Df, s"$runningPath/../../target/testoutput/adressesV1", List(), format="csv")

    adressesV1Df.createOrReplaceTempView("addrv1")

    val adressesV3Df = spark.sql(
      """SELECT domain,
            |collisionPicker(coalesce(fb_region_name,""), coalesce(gg_region_name,""), gg_fb_region_ratio) as region_name,
            |coalesce(gg_country_name, fb_country_name, wb_main_country) as country_name,
            |coalesce(gg_city, fb_city, wb_city) as city,
            |collisionPicker(coalesce(gg_address,""), coalesce(fb_address,""), gg_fb_address_ratio) as address,
            |collisionPicker(collisionPicker(coalesce(gg_phone,""), coalesce(fb_phone,""), gg_fb_phone_ratio),wb_phone,gg_wb_phone_ratio) as phone,
            |coalesce(gg_name, fb_name, wb_name) as company_name
            |FROM addrv1
            |""".stripMargin) //.show(100)

    writeOutput(adressesV3Df, s"$runningPath/../../target/testoutput/adressesV3", List(), format="csv")

    //Addresses V2
    val adressesDf = spark.sql(
      s"""SELECT region_code, region_name, country_code, country_name, city, address, company_name, category, array_join(collect_list(src),'-') AS list_src
         |FROM (
         |SELECT fb.region_code as region_code, fb.region_name as region_name, fb.country_code as country_code, fb.country_name as country_name, fb.city as city,
         |  fb.address as address, fb.name as company_name, fb.categories as category, "FB" as src
         |  FROM facebook_dataset fb
         |WHERE fb.address IS NOT NULL AND fb.name IS NOT NULL
         |UNION ALL
         |SELECT gg.region_code as region_code, gg.region_name as region_name, gg.country_code as country_code, gg.country_name as country_name, gg.city as city,
         |  gg.address as address, gg.name as company_name, gg.category as category, "GG" as src
         |  FROM google_dataset gg
         |WHERE gg.address IS NOT NULL AND gg.name IS NOT NULL
         |UNION ALL
         |SELECT NULL as region_code, wb.main_region as region_name, NULL as country_code, wb.main_country as country_name, wb.main_city as city,
         |  NULL as address, wb.legal_name as company_name, wb.s_category as category, "WB" as src
         |  FROM website_dataset wb
         |WHERE wb.legal_name IS NOT NULL
         |)
         |GROUP BY region_code, region_name, country_code, country_name, city, address, company_name, category
         |ORDER BY region_code, region_name, country_code, country_name, city, address, company_name, category
         |""".stripMargin)

    writeOutput(adressesDf, s"$runningPath/../../target/testoutput/adressesV2", List(), format="csv")
  }
*/
  test("check the logic") {
    val spark = SparkSession.builder().appName("Test").master("local[5]")
      .config("spark.driver.extraClassPath", "/opt/")
      .getOrCreate()

    Logic.runLogic(spark, s"$testPath", s"$runningPath/../../target/testoutput")
  }


}
