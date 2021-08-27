package cc.idx

import cc.warc._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, TimestampType, StringType, ShortType}

import org.archive.archivespark._
import org.archive.archivespark.specific.warc._
import org.archive.archivespark.specific.warc.functions._

import spark.session.AppSparkSession
import org.archive.archivespark.model.dataloads.ByteLoad

import org.archive.archivespark.sparkling.cdx.CdxRecord

import java.io.{InputStream, ByteArrayInputStream}

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
         * Creating SQL query to query the index dataframe
         */
        // val sqlQuery = "Select warc_filename, warc_record_offset, warc_record_length From " + viewName + " Where crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url RLIKE \'.*(/job/|/jobs/|/careers/|/career/).*\'"
        // val sqlQuery = "Select * From " + viewName + " Where crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url_host_tld=\'va\'"

        /**
         * Creating a SQL table from the index dataframe
         */
        df.createOrReplaceTempView(viewName)

        /**
         * Describing the table schema and running the query
         */
        // spark.sql("describe formatted " + viewName).show(10000)

        // spark.sql(sqlQuery).show(10, false)
        

        /**
          * Testing capacity to manually make a CdxRecord from ccindex table to select specific warc records
          */

        // val forCdxRec = df
        //     .select("url_surtkey","fetch_time","url","content_mime_type","fetch_status","content_digest","fetch_redirect","warc_segment","warc_record_length","warc_record_offset","warc_filename")
        //     .where("crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url RLIKE \'.*(/job/|/jobs/|/careers/|/career/).*\' AND content_mime_type = 'text/html'")

        //val rddWarc = WarcUtil.loadFiltered(forCdxRec).foreach(x => x.dataLoad(ByteLoad).)

        // rddWarc
            // .take(1)
            // .map(warc => cc.warc.SuperWarc(warc))
            // .foreach(warc => println(warc.toJsonString()))
        
        val forCdxRec = spark.sql("SELECT url_surtkey, fetch_time, url, content_mime_type, fetch_status, content_digest, fetch_redirect, warc_segment, warc_record_length, warc_record_offset, warc_filename"
            + " from " + viewName + " Where crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url RLIKE \'.*(/job/|/jobs/|/careers/|/career/).*\' LIMIT 1")

        val warc_rdd = WarcUtil.loadFiltered(forCdxRec)

        warc_rdd.map(warc => SuperWarc(warc)).take(5).foreach(warc => println(warc.payload()))

        spark.stop

        System.exit(0)
    }
}

object TestExtract {

    def main(args: Array[String]): Unit = {
        /**
         * Building the spark session
         */

        val spark = AppSparkSession()

        // val cdx_path = "/Users/vincey/downloads/def.cdx"
        // val root_path = "/Users/vincey/downloads"

        // val rdd_cdx = spark
        //     .sparkContext.textFile(cdx_path)
        //     .map(x => x.split(" "))
        //     .map(x => CdxRecord(x(0), x(1), x(2), x(3), x(4).toInt, x(5), x(6), x(7), x(8).toLong, Seq(x(9), x(10))))
        //     .map((_, root_path))

        // val warc_rdd = ArchiveSpark
        //         .load(WarcSpec.fromFiles(rdd_cdx))
        //         .enrich(WarcPayload)
        //         .enrich(cc.warc.WarcUtil.titleTextEnricher)
        //         .enrich(cc.warc.WarcUtil.bodyTextEnricher)

        // println(warc_rdd.peekJson)

        val tablePath = "s3a://commoncrawl/cc-index/table/cc-main/warc"

        val df = spark.read.schema(IndexUtil.schema).parquet(tablePath)

        val forCdxRec = df
            .select("url_surtkey","fetch_time","url","content_mime_type","fetch_status","content_digest","fetch_redirect","warc_segment","warc_record_length","warc_record_offset","warc_filename")
            .where("crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url_host_tld=\'va\'")
        
        val warc_rdd = WarcUtil.loadFiltered(forCdxRec)

        val stream = warc_rdd.take(1)(0).access[String]{s =>
            var httpResponse: InputStream = null
            val bytes = Array[Byte]()
            s.payload.read(bytes)
            bytes.foreach(println)
            ""
        }
        
        spark.stop

        System.exit(0)
    }
}