package cc.queries

import spark.session.AppSparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.SparkHadoopUtil
import cc.idx.{IndexUtil, FilteredIndex}
import cc.warc._

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

import scala.collection.mutable

object techJobs extends Queries {
    // def main(args: Array[String]): Unit = {
    //     val crawl = "CC-MAIN-2020-34"
    //     val bucketPath = "s3://maria-1086/TeamQueries/tech-job-posting-trends/"
    //     val spark = AppSparkSession()
    //     csvTrends(spark, "C:/Users/KyleJ/Downloads/CC-MAIN-2020-34-wordCount-RAW.csv", "C:/Users/KyleJ/scala/out")
    //     //wordCount(spark, crawl, bucketPath)
    // }
    /**
      * Creates a csv file of the number of wordcount matches grouped by date
      *
      * @param spark - `SparkSession` required to run
      * @param inPath - `String` path of the csv file containing the RAW data
      * @param outPath - `String` path to wher the output should be written to
      */
    def csvTrends(spark: SparkSession, inPath: String, outPath: String): Unit = {
        val rdd = spark.sparkContext.textFile(inPath)
            .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
            .map(line => line.split(","))
            .map(x => (x(0), x(1), x(2)))
        val df = spark.createDataFrame(rdd).toDF("date", "word", "count")
            .select(col("date"), col("word"), col("count").cast(IntegerType))
            .where(col("word").rlike("(?i)^(" + FilteredIndex.techJobTerms.mkString("|") + ").*$"))
            .groupBy(col("date"))
            .agg(sum(col("count")).alias("count_by_date"))
            .orderBy(col("date"))
        IndexUtil.write(df.toDF(), outPath + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss")), include_header = true, num_files = 1)
    }
    /**
      * Performs a Map-Reduce word count and writes the result to a csv in `path`
      *
      * @param spark - `SparkSession` required to run
      * @param crawl - `String` name of the crawl to run the count on.  Must be in IndexUtil.crawls
      * @param path - `String` path where the csv should be written
      */
    def wordCount(spark: SparkSession, crawl: String, path: String): Unit = {
        val warc_rdd = FilteredIndex.get(crawl)
        val warc_df = warc_rdd
            .map(warc => SuperWarc(warc))
            .map(warc => (warc.recordHeader("WARC-Date").substring(0,10), warc.payload(true).split(" ")))
            .flatMap(record => record._2.map(x => (record._1, x, 1)))
            .map { case (date, word, count) => ((date, word), count)}.reduceByKey(_+_)
            .map { case ((date, word), count) => (date, word, count) }
        val wordCount_df = spark.createDataFrame(warc_df).toDF("date", "word", "count")

        IndexUtil.write(wordCount_df, path + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss")), include_header=true, num_files=1)
    }
}