package cc.warc

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, DataFrame}

import spark.session.AppSparkSession

object CCWarcMain {

    def main(args: Array[String]): Unit = {
        /**
         * Building the spark session
         */

        val spark = AppSparkSession()

        val rdd = WarcUtil.load("s3a://commoncrawl/crawl-data/CC-MAIN-2021-31/segments/1627046157039.99/warc/CC-MAIN-20210805193327-20210805223327-00719.warc.gz")

        // Toy example to show all the accessible attributes on a SuperWarc
        rdd
        .take(3)
        .map(x => SuperWarc(x))
        .foreach { r =>
            println("+++ originalUrl: " + r.originalUrl)
            println("+++ mime: " + r.mime)
            println("+++ status: " + r.status)
            println("+++ toCdxString: " + r.toCdxString)
            println("+++ toJsonString: " + r.toJsonString)
            println("+++ recordHeader: " + r.recordHeader)
            println("+++ httpStatusLine: " + r.httpStatusLine)
            println("+++ httpHeader: " + r.httpHeader)
            println("+++ Payload: " + r.payload)
            println("+++ Payload (Text-only): " + r.payload(true))
            println()
        }
        
        spark.stop

        System.exit(0)
    }
}