package cc.queries

import cc.idx.IndexUtil
import spark.session.AppSparkSession
import org.archive.archivespark._
import org.archive.archivespark.functions._
import org.archive.archivespark.specific.warc._
import org.archive.archivespark.specific.warc.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

object infrequentJobs extends Queries {
  def main(args: Array[String]): Unit = {
    //     TODO:
    //        - Load Index CSV's of most recent crawl from S3 into dataframes (or 1 dataframe)
    //            - s3a://maria-1086/FilteredIndex/CC-MAIN-2021-31/*.csv
    //        - Count the number of records of each unique url_host_2nd_last_part
    //        - Count the number of records that have 3 or less postings
    //        - Divide that by the number of total unique posters

    val spark = AppSparkSession()
    val tPath = "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-31/*.csv"
    val df1 = spark.sqlContext.read.option("header", true).schema(IndexUtil.schema).csv(tPath)
    df1.createOrReplaceTempView("UniqueRec")
    //spark.sql("select * from UniqueRec").show()
    spark.sql("select count(distinct url_host_2nd_last_part) from UniqueRec").show()
    //val forCSV = spark.sql( "select distinct url_host_2nd_last_part, count(*) from UniqueRec").show()
  //val forCSV = spark.sql( "select distinct url_host_2nd_last_part, count(url_host_2nd_last_part) as total from UniqueRec  group by url_host_2nd_last_part having total <= 3")
    //println(forCSV.count)
    //      val first_table = forCSV.createOrReplaceTempView("1st_table")
//        spark.sql("select* from 1st_table")
//    spark.sql("select url_host_2nd_last_part from 1st_table where 'count(1)' <= 3").show(1000)



  }
}