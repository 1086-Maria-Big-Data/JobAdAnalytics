package cc.queries

import cc.idx.IndexUtil
import org.apache.spark.sql.functions.desc
import spark.session.AppSparkSession

object largestJob extends Queries {
def main(args: Array[String]): Unit = {
    run()
  }

  /**Reads in Common Crawl FilteredIndex files from S3 bucket, counting results grouped on index "url_host_name" column
    *
    */
  override def run(): Unit = {
    val spark = AppSparkSession()

    val df = spark.sqlContext.read
      .option("header", true)
      .schema(IndexUtil.schema)
      .csv("s3a://maria-1086/FilteredIndex/CC-MAIN-202*-**/*.csv")

    df.groupBy("url_host_name")
      .count()
      .orderBy(desc("count"))
      .show(20, false)

  }

}