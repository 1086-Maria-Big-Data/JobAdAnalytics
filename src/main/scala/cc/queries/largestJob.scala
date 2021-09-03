package cc.queries

import cc.idx.IndexUtil
import cc.warc.{SuperWarc, WarcUtil}
import org.apache.spark.sql.functions.{col, decode, desc, encode, exp, explode, lower}
import org.archive.archivespark.functions.{Html, HtmlText}
import spark.session.AppSparkSession
import sun.security.ssl.SSLContextImpl.DefaultSSLContext

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object largestJob extends Queries {
  def main(args: Array[String]): Unit = {
    val spark = AppSparkSession()

    //wordcount method

    //Step 1:

    // load job CSV
    val csv = spark.read
      //.format("csv")
      .option("header", true)
      .csv("s3a://maria-1086/FilteredIndex/CC-MAIN-2020-05/part-00000-451328f0-88e5-4cca-b2f8-703cd26ff421-c000.csv")

    val files = csv.select(col("warc_filename"))

    // query s3 cc path
    // loop all and perform wordcount. append output to one df
    var wordCount = spark.emptyDataFrame

 //   files.foreach{ i =>
      //println("s3a://commoncrawl/" + i.toString.replace("[", "").replace("]",""))

    //val rddAppend = WarcUtil.load("s3a://commoncrawl/" + i.toString.replace("[", "").replace("]",""))
    val rddAppend = WarcUtil.load("s3a://commoncrawl/" + files.first().toString.replace("[", "").replace("]",""))
      val wcAppend = spark.createDataFrame(
        rddAppend
          .take(5)
          .map(warc => SuperWarc(warc))
          .flatMap(warc => warc.payload(true).split(" "))
          .map(word => (word, 1))
          .groupBy(_._1)
          .map(pair => (pair._1, pair._2.map(_._2).reduce(_ + _)))
          .toSeq
      )
      wordCount = wordCount.union(wcAppend)
    //}

    //Step 2: filter for company names
    // 1. ascii only
    // 2. number and special characters
    // 3. common english articles
    // 4. common words that is not a company name
    // the goal is top50 has only company names and top100 has mostly only company names
/*
    wordCount
      .withColumnRenamed("_1", "word")
      .withColumnRenamed("_2", "count")
      .withColumn("word", lower(col("word")))

      //ascii encode+decode filter
      .withColumn("word", decode(encode(col("word") ,"ascii"),"ascii"))
      .filter(!col("word").contains("?"))

      //numbers and special characters
      .filter(!col("word").rlike("[0-9]"))
      .filter(!col("word").rlike("-|[/,.|#()&!*:]"))

      .orderBy(desc("count"))
      .show(100)

    //Step 3: mapping common words to url
    // may have url linked to multiple words
    // need to wait until the data output to see how to filter

    //Step 4: Query output

    //IndexUtil.write(wordCount_df, "s3://maria-1086/Hong-test/CCIdxMain/-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")), include_header=true, num_files=1)
*/
    spark.stop
    System.exit(0)
  }
}