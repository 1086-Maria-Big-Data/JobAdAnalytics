package cc.queries

import cc.idx.FilteredIndex.techJobTerms
import cc.idx.{CCIdxMain, FilteredIndex, IndexUtil}
import cc.warc.{SuperWarc, WarcUtil}
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import spark.session.AppSparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, DataFrame}



object largestJob extends Queries {
  def main(args: Array[String]) = {
   // FilteredIndex.view_sample()
    val spark = AppSparkSession()
    val csvRdd = spark.read.option("header",true).csv("s3a://maria-1086/Testing/Devin-Testing/outputs/test-write/part-00000-bb6fa4ba-4e14-49d3-985c-e570505dc35d-c000.csv")
    csvRdd.printSchema()
    csvRdd.repartition(6)
    //csvRdd.show(50, false)
    val warc_rdd = WarcUtil.loadFiltered(csvRdd)
    val superwarcs = warc_rdd.map(warc=>SuperWarc(warc))
    superwarcs.take(5).foreach(warc=>println(warc.originalUrl()))


    warc_rdd.take(25).map(warc=>SuperWarc(warc)).flatMap(warc=>warc.payload(true).split(" ")).foreach(println)

    val wordCount_df = spark.createDataFrame(
      warc_rdd
        .take(50)
        .map(warc => SuperWarc(warc))
        .flatMap(warc => warc.originalUrl.split("/"))
        .map(word => (word, 1))
        .groupBy(_._1)
        .map(pair => (pair._1, pair._2.map(_._2).reduce(_ + _)))
        .toSeq
    )

    wordCount_df.filter(col("_1").rlike("com")).show(500, false)
    //wordCount_df.orderBy(desc("_2")).show(500, false)





  }
  override def run(): Unit = {
  }




}