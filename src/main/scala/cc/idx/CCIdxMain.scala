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

import spark.session.AppSparkSession
import org.archive.archivespark.specific.warc.specs.WarcHdfsCdxRddSpec

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
    

    def main(args: Array[String]): Unit = {
        import org.archive.archivespark.sparkling.cdx.CdxRecord
        /**
         * Building the spark session
         */
        val spark = AppSparkSession()
        /**
         * loading the index to dataframe(df)
         */
        val df = spark.read.schema(schema).parquet(tablePath)
        df.printSchema()
        /**
         * Creating SQL query to query the index dataframe
         */
        val sqlQuery = "Select * From " + viewName + " Where crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url RLIKE \'.*(/job/|/jobs/|/careers/|/career/).*\'"
        // val sqlQuery = "Select * From " + viewName + " Where crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url_host_tld=\'va\'"

        /**
         * Creating a SQL table from the index dataframe
         */
        df.createOrReplaceTempView(viewName)

        /**
         * Describing the table schema and running the query
         */
        spark.sql("describe formatted " + viewName).show(10000)
        

        /**
          * Testing capacity to manually make a CdxRecord from ccindex table to select specific warc records
          */
        val forCdxRec = df.select("url_surtkey","fetch_time","url","content_mime_type","fetch_status","content_digest","warc_record_length","warc_record_offset","warc_filename").where("content_mime_type = 'text/html'").take(8)
        val arCdx = forCdxRec.map(c => (new CdxRecord(c.getAs[String](0),c(1).toString,c.getAs[String](2),c.getAs[String](3),c.getAs[Short](4).toInt,c.getAs[String](5),"-","-",c.getAs[Integer](6).toLong, Seq[String](c(7).toString,c.getAs[String](8))),"s3a://commoncrawl/"))
        val rddCdx = spark.sparkContext.parallelize(arCdx)

        val rddWarc = ArchiveSpark.load(WarcSpec.fromFiles(rddCdx))
        rddWarc.collect().foreach(warc => println(warc.toJsonString))

        //spark.stop

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
        val spark = AppSparkSession()

        val rdd = loadWARC("s3a://commoncrawl/crawl-data/CC-MAIN-2021-31/segments/1627046157039.99/warc/CC-MAIN-20210805193327-20210805223327-00719.warc.gz")
        .take(3).foreach(warc => println(warc.toCdxString))
        //.enrich(HtmlText.ofEach(Html.all("a")))
        //println(rdd.take(1)(0).toJsonString)
        
        spark.stop

        System.exit(0)
    }
}