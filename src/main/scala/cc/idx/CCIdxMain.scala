package cc.idx

import cc.warc._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._

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
        // val df = spark.read.schema(IndexUtil.schema).parquet(tablePath)
        
        /**
          * Testing capacity to manually make a CdxRecord from ccindex table to select specific warc records
          */

        // val forCdxRec = df
        //     .select("url_surtkey", "fetch_time", "url", "content_mime_type", "fetch_status", "content_digest", "fetch_redirect", "warc_segment", "warc_record_length", "warc_record_offset", "warc_filename")
        //     .where(col("crawl") === "CC-MAIN-2021-10" && col("subset") === "warc" && col("url_path").rlike(".*(/job/|/jobs/|/careers/|/career/).*"))

        // val warc_rdd = WarcUtil.loadFiltered(forCdxRec)

        // warc_rdd.map(warc => SuperWarc(warc)).take(5).foreach(warc => println(warc.payload(true)))

        val warc_rdd = WarcUtil.load("s3a://commoncrawl/crawl-data/CC-MAIN-2021-31/segments/1627046149929.88/warc/CC-MAIN-20210723143921-20210723173921-00000.warc.gz")
        warc_rdd.take(5).foreach(warc => println(warc.toJsonString))
    }
}

object TestExtract {

    def main(args: Array[String]): Unit = {
        /**
         * Building the spark session
         */

        val spark = AppSparkSession()

        val tablePath = "s3a://commoncrawl/cc-index/table/cc-main/warc"

        val df = spark.read.schema(IndexUtil.schema).parquet(tablePath)

        val forCdxRec = df
            .select("url_surtkey","fetch_time","url","content_mime_type","fetch_status","content_digest","fetch_redirect","warc_segment","warc_record_length","warc_record_offset","warc_filename")
            .where("crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url RLIKE \'.*(/job/|/jobs/|/careers/|/career/).*\'")
        
        forCdxRec.show(5)


        val warc_rdd = WarcUtil.loadFiltered(forCdxRec)

        import spark.implicits._

        val wordCount_df = spark.sparkContext.parallelize(warc_rdd.take(100)).map(warc => SuperWarc(warc)).flatMap(warc => warc.payload(true).split(" ")).map(word => (word,1)).reduceByKey(_ + _).map(pair => (pair._1,pair._2)).toDF("word","count")
        wordCount_df.show(5)
        IndexUtil.write(wordCount_df, "s3a://spark-submit-test/p3-test/write-test/" + args(0), include_header=true, single_file=true)
    }
}