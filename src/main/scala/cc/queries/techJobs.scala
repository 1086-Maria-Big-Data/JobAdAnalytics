package cc.queries

import spark.session.AppSparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import cc.idx.{IndexUtil, FilteredIndex}
import cc.warc._

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

import scala.collection.mutable

object techJobs extends Queries {
    def main(args: Array[String]): Unit = {
        val tablePath = "s3a://commoncrawl/cc-index/table/cc-main/warc"
        val bucketPath = "s3a://kw--bucket/p3/outs/"
        val spark = AppSparkSession()
        import org.archive.archivespark.sparkling.cdx.CdxRecord
        csvTrends(spark, "C:/Users/KyleJ/Downloads/CC-MAIN-31-wordCount-RAW.csv", "C:/Users/KyleJ/scala/out")
    }
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
        var new_df = df.coalesce(1)
        new_df.write.option("header", true).csv(outPath)
        //IndexUtil.write(df, outPath + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss")), include_header = true, num_files = 1 )
    }
    /**
      * Performs a Map-Reduce word count and writes the result to a csv in `path`
      *
      * @param spark - requires an active SparkSession to run
      * @param path - `String` path where the csv should be written
      */
    def wordCount(spark: SparkSession, path: String): Unit = {
        val warc_rdd = FilteredIndex.get()

        // val warc_df_take_5 = warc_rdd.take(5)
        // val warc_df = spark.sparkContext.parallelize(warc_df_take_5)
        val warc_df = warc_rdd
            .map(warc => SuperWarc(warc))
            .map(warc => (warc.recordHeader("WARC-Date").substring(0,10), warc.payload(true).split(" ")))
            .flatMap(record => record._2.map(x => (record._1, x, 1)))
            .map { case (date, word, count) => ((date, word), count)}.reduceByKey(_+_)
        // val initialSet = mutable.HashSet.empty[(Int, Int)]
        // val addToSet = (s: mutable.HashSet[(Int, Int)], v: (Int,Int)) => s += v
        // val mergePartitionSets = (p1: mutable.HashSet[(Int, Int)], p2: mutable.HashSet[(Int, Int)]) => p1 ++= p2
        // val uniqueByKey = warc_df.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
            .map { case ((date, word), count) => (date, word, count) }
        val wordCount_df = spark.createDataFrame(warc_df).toDF("date", "word", "count")

        IndexUtil.write(wordCount_df, path + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss")), include_header=true, num_files=1)
    }
    def wordCountFromCSV(spark: SparkSession, inPath: String, outPath: String): Unit = {
        val warc_rdd = FilteredIndex.get()

        // val warc_df_take_5 = warc_rdd.take(5)
        // val warc_df = spark.sparkContext.parallelize(warc_df_take_5)
        val warc_df = warc_rdd
            .map(warc => SuperWarc(warc))
            .map(warc => (warc.recordHeader("WARC-Date").substring(0,10), warc.payload(true).split(" ")))
            .flatMap(record => record._2.map(x => (record._1, x, 1)))
            .map { case (date, word, count) => ((date, word), count)}.reduceByKey(_+_)
        // val initialSet = mutable.HashSet.empty[(Int, Int)]
        // val addToSet = (s: mutable.HashSet[(Int, Int)], v: (Int,Int)) => s += v
        // val mergePartitionSets = (p1: mutable.HashSet[(Int, Int)], p2: mutable.HashSet[(Int, Int)]) => p1 ++= p2
        // val uniqueByKey = warc_df.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
            .map { case ((date, word), count) => (date, word, count) }
        val wordCount_df = spark.createDataFrame(warc_df).toDF("date", "word", "count")

        IndexUtil.write(wordCount_df, outPath + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss")), include_header=true, num_files=1)
    }
}