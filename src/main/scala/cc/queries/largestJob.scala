package cc.queries

import cc.idx.IndexUtil
import org.apache.spark.sql.functions._
import spark.session.AppSparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object largestJob extends Queries {
  def main(args: Array[String]) = {
    val spark = AppSparkSession()

    import spark.implicits._

    val df = spark.sqlContext.read
      .option("header", true)
      .schema(IndexUtil.schema)
      .csv("s3a://maria-1086/FilteredIndex/CC-MAIN-2021-**/*.csv")

    df.groupBy("url_host_name")
      .count
      .orderBy(desc("count"))
      .show(20, false)
    
    //IndexUtil.write(df, "s3://maria-1086/Testing/JesseTesting/outputs/" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")), include_header=true, num_files = 1)
  }
  override def run(): Unit = {

  }

}