package cc.idx

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, TimestampType, StringType, ShortType}

import org.archive.archivespark._
import org.archive.archivespark.specific.warc._
import org.archive.archivespark.specific.raw._
import org.archive.archivespark.functions._

import org.apache.log4j.Logger
import org.apache.log4j.Level

import appUtil.Util

/**
 * CCIdxMain is used for querying the index table from common crawl's S3 bucket
 *
 * TestExtract is an example of how to load WARC files
 */

object CCIdxMain {

    val props = Util.loadConfig()
    val access_key = props("AWS_ACCESS_KEY_ID")
    val access_secret = props("AWS_SECRET_ACCESS_KEY")

    /**
     * tablePath = the common crawl index's s3 bucket
     * viewName = name of common crawl index
     * schema = table structure for index
     */
    val tablePath = "s3a://commoncrawl/cc-index/table/cc-main/warc"
    val viewName = "ccindex"
    val schema = StructType(Array(
        StructField("url_surtkey", StringType, true),
        StructField("url", StringType, true),
        StructField("url_host_name", StringType, true),
        StructField("url_host_tld", StringType, true),
        StructField("url_host_2nd_last_part", StringType, true),
        StructField("url_host_3rd_last_part", StringType, true),
        StructField("url_host_4th_last_part", StringType, true),
        StructField("url_host_5th_last_part", StringType, true),
        StructField("url_host_registry_suffix", StringType, true),
        StructField("url_host_registered_domain", StringType, true),
        StructField("url_host_private_suffix", StringType, true),
        StructField("url_host_private_domain", StringType, true),
        StructField("url_protocol", StringType, true),
        StructField("url_port", IntegerType, true),
        StructField("url_path", StringType, true),
        StructField("url_query", StringType, true),
        StructField("fetch_time", TimestampType, true),
        StructField("fetch_status", ShortType, true),
        StructField("fetch_redirect", StringType, true),
        StructField("content_digest", StringType, true),
        StructField("content_mime_type", StringType, true),
        StructField("content_mime_detected", StringType, true),
        StructField("content_charset", StringType, true),
        StructField("content_languages", StringType, true),
        StructField("content_truncated", StringType, true),
        StructField("warc_filename", StringType, true),
        StructField("warc_record_offset", IntegerType, true),
        StructField("warc_record_length", IntegerType, true),
        StructField("warc_segment", StringType, true),
        StructField("crawl", StringType, true),
        StructField("subset", StringType, true)
    ))
    /**
     * Setting spark configurations
     */
    val conf = new SparkConf()
        .setAppName(this.getClass.getCanonicalName())
        .set("spark.hadoop.parquet.enable.dictionary", "true")
        .set("spark.hadoop.parquet.enable.summary-metadata", "true")
        .set("spark.sql.hive.metastorePartitionPruning", "true")
        .set("spark.sql.parquet.filterPushdown", "true")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.executor.userClassPathFirst", "true")

    def main(args: Array[String]): Unit = {
        /**
         * Building the spark session
         */
        val spark = SparkSession.builder.master("local[*]")
            .config(conf)
            .getOrCreate

        val config = spark.sparkContext.hadoopConfiguration
            config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
            config.set("fs.s3a.awsAccessKeyId", access_key)
            config.set("fs.s3a.awsSecretAccessKey", access_secret)
        /**
         * loading the index to dataframe(df)
         */
        val df = spark.read.schema(schema).parquet(tablePath)
        df.printSchema()
        /**
         * Creating SQL query to query the index dataframe
         */
        //val sqlQuery = "Select * From " + viewName + " Where crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url RLIKE \'.*(/job/|/jobs/|/careers/|/career/).*\'"
        val sqlQuery = "Select url, content_languages From " + viewName + " Where crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url_host_tld=\'va\'"

        /**
         * Creating a SQL table from the index dataframe
         */
        df.createOrReplaceTempView(viewName)

        /**
         * Describing the table schema and running the query
         */
        spark.sql("describe formatted " + viewName).show(10000)
        spark.sql(sqlQuery).show(100)

        spark.stop

        System.exit(0)
    }
}

object TestExtract {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    def loadWARC(path: String): RDD[WarcRecord] = {
        return ArchiveSpark.load(WarcSpec.fromFiles(path))
    }

    def main(args: Array[String]): Unit = {
        /**
         * Building the spark session
         */
        val spark = SparkSession.builder.master("local[*]")
            .config(CCIdxMain.conf)
            .getOrCreate

        val config = spark.sparkContext.hadoopConfiguration
        config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        config.set("fs.s3a.awsAccessKeyId", CCIdxMain.access_key)
        config.set("fs.s3a.awsSecretAccessKey", CCIdxMain.access_secret)

        val rdd = loadWARC("s3a://commoncrawl/crawl-data/CC-MAIN-2021-31/segments/1627046157039.99/warc/CC-MAIN-20210805193327-20210805223327-00719.warc.gz").enrich(HtmlText.ofEach(Html.all("a")))
        println(rdd.take(1)(0).toJsonString)
        
        spark.stop

        System.exit(0)
    }
}