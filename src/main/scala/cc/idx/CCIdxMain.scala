package cc.idx

import cc.warc._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, TimestampType, StringType, ShortType}

import org.archive.archivespark._
import org.archive.archivespark.specific.warc._
import org.archive.archivespark.specific.warc.functions._

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
    val tablePath = "s3://commoncrawl/cc-index/table/cc-main/warc"
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
        df.printSchema()
        /**
         * Creating SQL query to query the index dataframe
         */
        // val sqlQuery = "Select warc_filename, warc_record_offset, warc_record_length From " + viewName + " Where crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url RLIKE \'.*(/job/|/jobs/|/careers/|/career/).*\'"
        // val sqlQuery = "Select * From " + viewName + " Where crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url_host_tld=\'va\'"

        /**
         * Creating a SQL table from the index dataframe
         */
        // df.createOrReplaceTempView(viewName)

        /**
         * Describing the table schema and running the query
         */
        // spark.sql("describe formatted " + viewName).show(10000)

        // spark.sql(sqlQuery).show(10, false)
        

        /**
          * Testing capacity to manually make a CdxRecord from ccindex table to select specific warc records
          */

        val forCdxRec = df
            .select("url_surtkey","fetch_time","url","content_mime_type","fetch_status","content_digest","fetch_redirect","warc_segment","warc_record_length","warc_record_offset","warc_filename")
            .where("crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url RLIKE \'.*(/job/|/jobs/|/careers/|/career/).*\' AND content_mime_type = 'text/html'")

        val rddWarc = WarcUtil.loadFiltered(forCdxRec)
                .enrich(WarcPayload)
                .enrich(cc.warc.WarcUtil.titleTextEnricher)
                .enrich(cc.warc.WarcUtil.bodyTextEnricher)

        rddWarc
            .take(4)
            .map(warc => cc.warc.SuperWarc(warc))
            .foreach(warc => println(warc.toJsonString()))

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

        val spark = AppSparkSession()

        val rdd = loadWARC("s3a://commoncrawl/crawl-data/CC-MAIN-2021-31/segments/1627046157039.99/warc/CC-MAIN-20210805193327-20210805223327-00719.warc.gz")
        
        spark.stop

        System.exit(0)
    }
}