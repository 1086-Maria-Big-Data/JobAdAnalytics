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
import org.apache.spark.sql.functions.{quarter, year, month, col}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, StringType, TimestampType, ShortType}
import cc.idx._
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import org.apache.spark.sql.SparkSession


object jobSpikes extends Queries {

    /**
      * Builds a Parquet using the original filtered index, partitioned by fetch year & month.
      *
      * @param spark - SparkSession instance
      */
    def buildIndexPartitions(spark: SparkSession): Unit = {
        partitionByMonthAndYear(
            spark, 
            "s3a://maria-1086/FilteredIndex/CC-MAIN-202*-**/*.csv", 
            "s3a://maria-1086/TeamQueries/job-posting-spikes/parquet"
        )
    }
    
    /**
      * Given a source DataFrame with the fetch_time column, creates a parquet that is partitioned by fetch year and month, extracted from fetch_time.
      *
      * @param spark - SparkSession instance
      * @param srcPath - the source path for the DataFrame
      * @param dstPath - the destination path to write the parquet
      */
    def partitionByMonthAndYear(spark: SparkSession, srcPath: String, dstPath: String): Unit = {
        val df = spark.sqlContext
            .read
            .option("header", true)
            .schema(IndexUtil.schema)
            .csv(srcPath)
            .withColumn("fetch_year", year(col("fetch_time")))
            .withColumn("fetch_month", month(col("fetch_time")))

        IndexUtil.writeParquet(df, dstPath, partition_cols=Seq("fetch_year", "fetch_month"))
    }

    /**
      * Using the partitioned parquet, aggregates record count, grouping them by fetch_year and fetch_month.
      *
      * @param spark - SparkSession instance
      * @return a DataFrame where each row is the aggregated row count grouped by fetch_year and fetch_month.
      */
    def countByMonth(spark: SparkSession): DataFrame = {
        val df = spark.sqlContext.read.option("header", true)
            .schema(IndexUtil.schema)
            .parquet("s3a://maria-1086/TeamQueries/job-posting-spikes/parquet/")

        return df
            .groupBy("fetch_year", "fetch_month")
            .count
            .orderBy("fetch_year", "fetch_month")
    }

    /**
      * Using the partitioned parquet, aggregates record count, grouping them by fetch_year and fetch_month.
      *
      * @param spark - SparkSession instance
      * @return a DataFrame where each row is the aggregated row count grouped by fetch_year and fetch_month.
      */
    def countByQuarter(spark: SparkSession): DataFrame ={
        val df = spark.sqlContext
            .read
            .option("header", true)
            .schema(IndexUtil.schema)
            .parquet("s3a://maria-1086/TeamQueries/job-posting-spikes/parquet/")
            .withColumn("fetch_quarter", quarter(col("fetch_time")))

        return df
            .groupBy("fetch_year", "fetch_quarter")
            .count
            .orderBy("fetch_year", "fetch_quarter")
    }

    def jobSpikesByJob(spark: SparkSession): Unit = {
        val df = spark.read.format("parquet").option("header", "true").load("s3a://maria-1086/TeamQueries/job-posting-spikes/parquet")
        df.createOrReplaceTempView("dat")
        val df_jobs = spark.sql("SELECT (select count(url) from dat where LOWER(url) like '%java%' and LOWER(url) not like '%javascript%') as java, (SELECT count(url) from dat where LOWER(url) like '%python%') as python, (SELECT count(url) from dat where LOWER(url) like '%scala%') as scala, (SELECT count(url) from dat where LOWER(url) like '%matlab%') as matlab, (SELECT count(url) from dat where LOWER(url) like '%sql%') as sql from dat limit 1")
        IndexUtil.writeParquet(df_jobs, "s3a://maria-1086/TeamQueries/job-posting-spikes/mark_parquet/zone")
        
    }
        
    def main(args: Array[String]): Unit = {
        
        val spark = AppSparkSession()
        val df = spark.sqlContext
            .read
            .option("basePath", "s3a://maria-1086/TeamQueries/job-posting-spikes/parquet")
            .parquet("s3a://maria-1086/TeamQueries/job-posting-spikes/parquet/")

        jobSpikesByJob(spark)
    }
}

