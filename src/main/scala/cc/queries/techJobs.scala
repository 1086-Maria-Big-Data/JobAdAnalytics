package cc.queries

import spark.session.AppSparkSession
import org.apache.spark.sql.functions._
import cc.idx.IndexUtil
import cc.warc._

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

import scala.collection.mutable

object techJobs extends Queries {
    def main(args: Array[String]): Unit = {
        run()
    }
    override def run(): Unit = {
        //Is there a general trend in tech job postings over the past year? What about the past month?
        val tablePath = "s3a://commoncrawl/cc-index/table/cc-main/warc"
        val bucketPath = "s3a://kw--bucket/p3/outs/"
        import org.archive.archivespark.sparkling.cdx.CdxRecord
        /**
         * Building the spark session
         */
        val spark = AppSparkSession()
        /**
         * loading the index to dataframe(df)
         */
        val df = spark.read.schema(IndexUtil.schema).parquet(tablePath)
        
        /**
          * Testing capacity to manually make a CdxRecord from ccindex table to select specific warc records
          */

        val forCdxRec = df
            .select("url_surtkey", "fetch_time", "url", "content_mime_type", "fetch_status", "content_digest", "fetch_redirect", "warc_segment", "warc_record_length", "warc_record_offset", "warc_filename")
            .where(col("fetch_status") === 200 && col("crawl") === "CC-MAIN-2021-10" && col("subset") === "warc" && col("content_languages") === "eng" && col("url_path").rlike(".*(/job/|/jobs/|/careers/|/career/).*"))

        val warc_rdd = WarcUtil.loadFiltered(forCdxRec)
        // val schema =
        // StructType(Array(
        //     StructField("date", StringType, false),
        //     StructField("word", StringType, false),
        //     StructField("count", IntegerType, true)))

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
            //.take(5)
            //.groupBy(_._1)
            //.map(record => (record._1, record._2.map(_._1).reduce(_+_), record._2.map(_._3).reduce(_ + _)))
            //.toSeq
        val wordCount_df = spark.createDataFrame(warc_df).toDF("date", "word", "count")

        IndexUtil.write(wordCount_df, bucketPath + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss")), include_header=true, num_files=1)
    }
}