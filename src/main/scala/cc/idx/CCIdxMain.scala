package cc.idx

import cc.warc._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._

import org.archive.archivespark._
import org.archive.archivespark.specific.warc._
import org.archive.archivespark.specific.warc.functions._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import spark.session.AppSparkSession

/**
 * CCIdxMain is used for querying the index table from common crawl's S3 bucket
 *
 * TestExtract is an example of how to load WARC files
 */

object CCIdxMain {
    /**
     * tablePath = the common crawl index's s3 bucket
     * viewName = name of common crawl index
     */
    val tablePath = "s3a://commoncrawl/cc-index/table/cc-main/warc"
    val viewName = "ccindex"
    /**
     * Setting spark configurations
     */
    

    def main(args: Array[String]): Unit = {
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
            .where(col("crawl") === "CC-MAIN-2021-10" && col("subset") === "warc" && col("url_path").rlike(".*(/job/|/jobs/|/careers/|/career/).*"))

        val warc_rdd = WarcUtil.loadFiltered(forCdxRec)

        warc_rdd.map(warc => SuperWarc(warc)).take(5).foreach(warc => println(warc.payload(true)))

        spark.stop

        System.exit(0)
    }
}

object TestExtract {

    def main(args: Array[String]): Unit = {
        val spark = AppSparkSession()

        /**
          * Example 1: Load one WARC file from CommonCrawl S3, perform word count on 5 records and write as CSV to S3 bucket.
          */
        val warc_rdd = WarcUtil.load("s3a://commoncrawl/crawl-data/CC-MAIN-2021-31/segments/1627046149929.88/warc/CC-MAIN-20210723143921-20210723173921-00000.warc.gz")

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

        IndexUtil.write(wordCount_df, "s3a://vince-bucket/p3/outs/" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")), include_header=true, num_files=1)

        /**
          * Example 2: Query CommonCrawl Index, get WARC record offsets, perform word count on all records and write as CSV to S3 bucket.
          */

        val tablePath = "s3a://commoncrawl/cc-index/table/cc-main/warc"

        val df = spark.read.schema(IndexUtil.schema).parquet(tablePath)

        val forCdxRec = df
            .select("url_surtkey", "fetch_time", "url", "content_mime_type", "fetch_status", "content_digest", "fetch_redirect", "warc_segment", "warc_record_length", "warc_record_offset", "warc_filename")
            .where(col("crawl") === "CC-MAIN-2021-10" && col("subset") === "warc" && col("content_languages") === "eng" && col("url_path").rlike(".*(/job/|/jobs/|/careers/|/career/).*"))

        val warc_rdd2 = WarcUtil.loadFiltered(forCdxRec).repartition(640)

        val wordCount_df2 = spark.createDataFrame(
            warc_rdd2
                .map(warc => SuperWarc(warc))
                .flatMap(warc => warc.payload(true).split(" "))
                .map(word => (word, 1))
                .reduceByKey(_ + _)
        )

        IndexUtil.write(wordCount_df2, "s3://maria-1086/Vince-Test-CCIdxMain/outputs/" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")), include_header=true, num_files=64)
    }
}