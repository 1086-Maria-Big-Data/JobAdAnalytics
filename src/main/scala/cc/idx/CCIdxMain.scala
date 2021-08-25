package cc.idx

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, TimestampType, StringType, ShortType}

import org.archive.archivespark._
import org.archive.archivespark.specific.warc._

/**
 * CCIdxMain is used for querying the index table from common crawl's S3 bucket
 *
 * TestExtract is an example of how to load WARC files
 */

object CCIdxMain {
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
//Warning main method will take about 30 minutes to run, we should try to optimize
    def main(args: Array[String]): Unit = {
        /**
         * Building the spark session
         */
        val spark = SparkSession.builder.master("local[*]")
          .config(conf)
          .getOrCreate
        /**
         * loading the index to dataframe(df)
         */
        val df = spark.read.schema(schema).parquet(tablePath)
        df.printSchema()
        /**
         * Creating SQL query to query the index dataframe
         */
        //val sqlQuery = "Select * From " + viewName + " Where crawl=\'CC-MAIN-2021-10\' and url RLIKE \'.*(/job/|/jobs/|/careers/|/career/).*\' and content_languages=\'eng\' and url_host_tld in (\'com\',\'net\',\'org\',\'edu\')"
        val sqlQuery = "Select url, warc_filename From " + viewName + " Where crawl=\'CC-MAIN-2021-10\' And content_languages=\'eng\' AND url_host_tld=\'com\'"

        /**
         * Creating a SQL table from the index dataframe
         */
        df.createOrReplaceTempView(viewName)

        /**
         * Describing the table schema and running the query
         */
        spark.sql("describe formatted " + viewName).show(10000)
        val firstFilter = spark.sql(sqlQuery)
          firstFilter.show(100)
        firstFilter.createOrReplaceTempView("firstFilter")
        //Second Query to filter for job sites
        val query2 = "SELECT * From firstFilter where url RLIKE \'.*(/job/|/jobs/|/careers/|/career/).*\'"
        val jobsFilter = spark.sql(query2)
        jobsFilter.show(10)

        spark.stop

        System.exit(0)
    }
}

object TestExtract {

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
        /**
         * Creating an RDD of your downloaded WARC file
         */
        val rdd = loadWARC("/Users/grant/downloads/CC-MAIN-20180116070444-20180116090444-00000.warc")

        println(rdd.peekJson)

        spark.stop

        System.exit(0)
    }
}