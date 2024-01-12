package com.veridion.bigdata.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import me.xdrop.fuzzywuzzy._

object FuzzyUtil extends Logging{

  def init(spark: SparkSession): Unit = {

    val normalizeSentenceUdf = udf(normalizeSentence _)
    spark.udf.register("normalizeSentence", normalizeSentenceUdf)

    val fuzzySortedPartialRatioUdf = udf(fuzzySortedPartialRatio _)
    spark.udf.register("fuzzySortedPartialRatio", fuzzySortedPartialRatioUdf)

    val collisionPickerUdf = udf(collisionPicker _)
    spark.udf.register("collisionPicker", collisionPickerUdf)

    println("Initialized")
  }

  def normalizeSentence(s1: String): String = {
    s1.toLowerCase()
  }

  def fuzzySortedPartialRatio(s1:String, s2:String):Int = {
    FuzzySearch.tokenSortPartialRatio(s1, s2)
  }

  def collisionPicker(s1: String, s2: String, ratio: Int): String = ratio match {
    case 100 => s1 //matches
    case x if (0 < x && x < 50) => s1 //first dataset is more full -> pick from there
    case x if (x >=50 && x < 100) => if (!s1.isEmpty) s1 else s2 // actually don't know about that case
    case 0 => if (!s1.isEmpty) s1 else s2 //absolutely not matches but maybe the second dataset contains the row
  }


}
