package cc.queries

import cc.idx.IndexUtil
import spark.session.AppSparkSession
import org.archive.archivespark._
import org.archive.archivespark.functions._
import org.archive.archivespark.specific.warc._
import org.archive.archivespark.specific.warc.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

object infrequentJobs extends Queries {
  def main(args: Array[String]): Unit = {
    val spark = AppSparkSession()

    val crawls = Array("s3a://maria-1086/FilteredIndex/CC-MAIN-2020-05/*.csv",
      "s3a://maria-1086/FilteredIndex/CC-MAIN-2020-10/*.csv",
      "s3a://maria-1086/FilteredIndex/CC-MAIN-2020-16/*.csv",
      "s3a://maria-1086/FilteredIndex/CC-MAIN-2020-24/*.csv",
      "s3a://maria-1086/FilteredIndex/CC-MAIN-2020-29/*.csv",
      "s3a://maria-1086/FilteredIndex/CC-MAIN-2020-34/*.csv",
      "s3a://maria-1086/FilteredIndex/CC-MAIN-2020-40/*.csv",
      "s3a://maria-1086/FilteredIndex/CC-MAIN-2020-45/*.csv",
      "s3a://maria-1086/FilteredIndex/CC-MAIN-2020-50/*.csv",
      "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-04/*.csv",
      "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-10/*.csv",
      "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-17/*.csv",
      "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-21/*.csv",
      "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-25/*.csv",
      "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-31/*.csv")

    val months = Array("01-2020",
      "02-2020",
      "03-2020",
      "05-2020",
      "07-2020",
      "08-2020",
      "09-2020",
      "10-2020",
      "11-2020",
      "01-2021",
      "02-2021",
      "04-2021",
      "05-2021",
      "06-2021",
      "07-2021")

    val percentages = new ArrayBuffer[Float]()

    for(i<- 0 to crawls.length - 1) {
      percentages += get_Infrequent_Perc(crawls(i))
    }

    val mapMoPerc = scala.collection.mutable.Map[String, Float]()

    for(i <- 0 to percentages.length - 1) {
      mapMoPerc += Tuple2[String, Float](months(i), percentages(i))
    }
    val percentagesDF = spark.createDataFrame(mapMoPerc.toSeq).toDF("Month", "Percentage of Infrequent Posters")

    IndexUtil.write(percentagesDF, "s3a://maria-1086/Testing/gabriel-testing/infrequent-out" , include_header=true, num_files = 1)
  }

  /** Returns the percentage of Infrequent Job Posters (companies that post 3 or less jobs per month) given
    * a csv index of a crawl.
    *
    * @param path The file path to the csv to be read
    * @return
    */
  def get_Infrequent_Perc(path: String): Float = {
    val spark = AppSparkSession()
    val tPath = path
    var percentage = -1.0.toFloat

    try {
      val df1 = spark.sqlContext.read.option("header", true).schema(IndexUtil.schema).csv(tPath)
      df1.createOrReplaceTempView("UniqueRec")
      val totalCompanies = spark.sql("select distinct url_host_2nd_last_part from UniqueRec").count()
      val infrequentPosters = spark.sql("SELECT DISTINCT url_host_2nd_last_part, count(url_host_2nd_last_part)" +
        " as total FROM UniqueRec GROUP BY url_host_2nd_last_part HAVING total <= 3").count()
      percentage = infrequentPosters.toFloat / totalCompanies.toFloat * 100
    } catch {
      case e: Throwable => println("Could not load " + path)
    }

    return percentage
  }
}