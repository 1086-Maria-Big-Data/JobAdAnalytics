package cc.queries

import spark.session.AppSparkSession
import org.archive.archivespark._
import org.archive.archivespark.functions._
import org.archive.archivespark.specific.warc._
import org.archive.archivespark.specific.warc.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

object infrequentJobs extends Queries {
  def main(args: Array[String]): Unit = {
    /* TODO:
        - Load Index CSV's of most recent crawl from S3 into dataframes (or 1 dataframe)
            - s3a://maria-1086/FilteredIndex/CC-MAIN-2021-31/*.csv
        - Count the number of records of each unique url_host_2nd_last_part
        - Count the number of records that have 3 or less postings
        - Divide that by the number of total unique posters
     */
  }

}