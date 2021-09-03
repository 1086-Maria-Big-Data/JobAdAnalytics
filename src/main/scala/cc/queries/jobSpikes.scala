package cc.queries
import spark.session.AppSparkSession 
import org.apache.spark.sql.functions.col

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
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime


object jobSpikes extends Queries {
    
    def partitionByMonth(spark: SparkSession): Unit = {
        val df = spark.sqlContext.read.option("header", true).schema(IndexUtil.schema).csv("s3a://maria-1086/FilteredIndex/CC-MAIN-2020-**/*.csv")
        var newDF = null.asInstanceOf[DataFrame]
        for(x <- 1 to 12){
            newDF = df.where(month(col("fetch_time"))===x)
            IndexUtil.write(newDF, "s3a://maria-1086/TeamQueries/job-posting-spikes/2020/%02d".format(x), include_header=true, num_files=8)
        }
    }
    def countByMonth(spark: SparkSession): Unit ={
        val df = spark.sqlContext.read.option("header", true).schema(IndexUtil.schema).parquet("s3://maria-1086/TeamQueries/job-posting-spikes/2021/**/*.parquet")

        
    }

    def jobSpikesByJob(spark: SparkSession, path: String): Unit = {
        val df = spark.read.format("csv").option("header", "true").load(path)
        val df2 = 
        df.createOrReplaceTempView("dat")
        spark.sql("SELECT (select count(url) from dat where LOWER(url) like '%java%' and LOWER(url) not like '%javascript%') as java, (SELECT count(url) from dat where LOWER(url) like '%python%') as python, (SELECT count(url) from dat where LOWER(url) like '%scala%') as scala, (SELECT count(url) from dat where LOWER(url) like '%matlab%') as matlab, (SELECT count(url) from dat where LOWER(url) like '%sql%') as sql from dat limit 1").show()
    }
        
    def main(args: Array[String]): Unit = {
        val spark = AppSparkSession()
        partitionByMonth(spark)
    }
}