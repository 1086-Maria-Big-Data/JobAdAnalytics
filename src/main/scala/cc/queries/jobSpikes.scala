package cc.queries

import cc.warc._
import spark.session.AppSparkSession
import org.archive.archivespark._
import org.archive.archivespark.functions._
import org.archive.archivespark.specific.warc._
import org.archive.archivespark.specific.warc.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{month, col}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, StringType, TimestampType, ShortType}
import cc.idx._
import org.apache.spark.sql.Row
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime


object jobSpikes extends Queries {
    val schema = StructType(Array(
        StructField("url_surtkey", StringType, true),
        StructField("fetch_time", TimestampType, true),
        StructField("url", StringType, true),
        StructField("content_mime_type", StringType, true),
        StructField("fetch_status", ShortType, true),
        StructField("content_digest", StringType, true),
        StructField("fetch_redirect", StringType, true),        
        StructField("warc_segment", StringType, true),
        StructField("warc_record_length", IntegerType, true),
        StructField("warc_record_offset", IntegerType, true),
        StructField("warc_filename", StringType, true)
    ))

    def partitionByMonth(): Unit = {
        val spark = AppSparkSession()
        val rddFile = spark.sparkContext.wholeTextFiles("s3a://maria-1086/Testing/Devin-Testing/outputs/test-write/*.csv", 24).flatMap(_._2.split("\n").drop(1).map(x=>x.split(" "))).map(x=>Row(x(0),LocalDateTime.parse(x(1).substring(0, 10), DateTimeFormatter.ISO_LOCAL_DATE),x(2),x(3),x(4),x(5),x(6),x(7),x(8).toInt,x(9).toInt,x(10)))
        val DFFile = spark.createDataFrame(rddFile, schema)
        DFFile.show()
        // val testDF = spark.read.format("csv").schema(schema).option("header", "true").load("s3a://maria-1086/Testing/Mark-Testing/part-00001-bb6fa4ba-4e14-49d3-985c-e570505dc35d-c000.csv")
        // // testDF.createOrReplaceTempView("testTable")
        // val testDF1 = testDF.where(month(col("fetch_time"))===2)
        // IndexUtil.write(testDF1, "s3a://maria-1086/TeamQueries/job-posting-spikes/2021//", include_header=true, num_files=1)
    }
    def main(args: Array[String]){
        partitionByMonth()
    }
}