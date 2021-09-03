package cc.queries

import cc.idx.IndexUtil
import cc.warc.{SuperWarc, WarcUtil}
import org.apache.spark.sql.functions.{col, to_date}
import org.archive.archivespark.functions.{Html, HtmlText}
import org.archive.archivespark.sparkling.cdx.CdxRecord
import org.archive.archivespark.specific.warc.WarcRecord
import spark.session.AppSparkSession
import cc.idx.FilteredIndex

object techJobs extends Queries {
  def main(args: Array[String]): Unit = {
    val spark = AppSparkSession()
    val df = spark.read.options(Map("header" -> "true")).csv("s3a://maria-1086/Testing/Devin-Testing/outputs/test-write/")

    val withDate = df.withColumn("fetch_date", to_date(col("fetch_time"),"yyyy-MM-dd"))

    val warc_rdd = WarcUtil.loadFiltered(withDate)

    println(warc_rdd.take(2))
    val wordCount_df = spark.createDataFrame(
      warc_rdd
        .take(5)
        .map(warc => SuperWarc(warc))
        .flatMap(warc => warc.payload(true).split(" "))
        .map(word => (word, 1))
        .groupBy(_._1)
        .map(pair => (pair._1, pair._2.map(_._2).reduce(_ + _)))
        .toSeq
    )

    wordCount_df.show()

    val grouped = withDate.groupBy("fetch_date").count().orderBy("fetch_date")

    //grouped.show()
    //withDate.printSchema()
    //withDate.show(1)

  }
    
}