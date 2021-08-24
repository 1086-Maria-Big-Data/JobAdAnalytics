package cc.idx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, TimestampType, StringType, ShortType}

object CCIdxMain {
    def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName(this.getClass.getCanonicalName())
			.set("spark.hadoop.parquet.enable.dictionary", "true")
			.set("spark.hadoop.parquet.enable.summary-metadata", "true")
			.set("spark.sql.hive.metastorePartitionPruning", "true")
			.set("spark.sql.parquet.filterPushdown", "true")

		val spark = SparkSession.builder.master("local[*]")
			.config(conf)
			.getOrCreate

		val sc = spark.sparkContext

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
		val df = spark.read.schema(schema).parquet(tablePath)
		df.printSchema()
		//val sqlQuery = "Select * From " + viewName + " Where crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url RLIKE \'.*(/job/|/jobs/|/careers/|/career/).*\'"
		val sqlQuery = "Select url, content_languages From " + viewName + " Where crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url_host_tld=\'va\'"
		df.createOrReplaceTempView(viewName)

		spark.sql("describe formatted " + viewName).show(10000)
		spark.sql(sqlQuery).show(100)

		spark.stop

        System.exit(0)
    }
}