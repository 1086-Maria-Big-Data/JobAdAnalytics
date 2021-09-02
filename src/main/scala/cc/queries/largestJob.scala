package cc.queries

import cc.idx.IndexUtil
import cc.warc.{SuperWarc, WarcUtil}
import org.apache.spark.sql.functions.desc
import org.archive.archivespark.functions.{Html, HtmlText}
import spark.session.AppSparkSession
import sun.security.ssl.SSLContextImpl.DefaultSSLContext

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object largestJob extends Queries {


  def main(args: Array[String]): Unit = {
    val spark = AppSparkSession()

    //wordcount method
    val rdd = WarcUtil.load("s3a://commoncrawl/crawl-data/CC-MAIN-2014-23/segments/1405997885796.93/warc/CC-MAIN-20140722025805-00016-ip-10-33-131-23.ec2.internal.warc.gz")
    val wordCount_df = spark.createDataFrame(
      rdd
        .take(5)
        .map(warc => SuperWarc(warc))
        .flatMap(warc => warc.payload(true).split(" "))
        .map(word => (word, 1))
        .groupBy(_._1)
        .map(pair => (pair._1, pair._2.map(_._2).reduce(_ + _)))
        .toSeq
    )


    wordCount_df
      .withColumnRenamed("_1", "word")
      .withColumnRenamed("_2", "count")
      .orderBy(desc("count"))
      .show()

    //IndexUtil.write(wordCount_df, "s3://maria-1086/Hong-test/CCIdxMain/-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")), include_header=true, num_files=1)

    spark.stop
    System.exit(0)
  }
}