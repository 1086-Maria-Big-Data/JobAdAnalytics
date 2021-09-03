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
import org.apache.spark.sql.functions.{year, month, col}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, StringType, TimestampType, ShortType}
import cc.idx._
import org.apache.spark.sql.DataFrame
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

object jobSpikes extends Queries {

    val spark = AppSparkSession()
    
    def partitionByMonthAndYear(): Unit = {
        val spark = AppSparkSession()
        val df = spark.sqlContext
            .read
            .option("header", true)
            .schema(IndexUtil.schema)
            .csv("s3a://maria-1086/FilteredIndex/CC-MAIN-202*-**/*.csv")
            .withColumn("fetch_year", year(col("fetch_time")))
            .withColumn("fetch_month", month(col("fetch_time")))

        IndexUtil.writeParquet(df, "s3a://maria-1086/TeamQueries/job-posting-spikes/parquet", partition_cols=Seq("fetch_year", "fetch_month"))
    }

    def jobSpikesByJob(): Unit = {
        val spark = AppSparkSession()
        //val df = spark.sqlContext.read.option("header", true).schema(IndexUtil.schema).csv("s3a://maria-1086/TeamQueries/job-posting-spikes/2021/**/*.csv")
        val df = spark.read.format("csv").option("header", "true").load("s3a://maria-1086/TeamQueries/job-posting-spikes/2021/**/*.csv")
        for(x <- 1 to 12){
            df.createOrReplaceTempView("dat")
            val df_jobs = spark.sql("SELECT (select count(url) from dat where LOWER(url) like '%java%' and LOWER(url) not like '%javascript%') as java, (SELECT count(url) from dat where LOWER(url) like '%python%') as python, (SELECT count(url) from dat where LOWER(url) like '%scala%') as scala, (SELECT count(url) from dat where LOWER(url) like '%matlab%') as matlab, (SELECT count(url) from dat where LOWER(url) like '%sql%') as sql from dat limit 1")
            IndexUtil.write(df_jobs, "s3a://maria-1086/TeamQueries/job-posting-spikes/mark_test/2021/%02".format(x))
        }
    }
        
    def main(args: Array[String]): Unit = {
        // jobSpikesByJob()
        partitionByMonthAndYear()
    }
}