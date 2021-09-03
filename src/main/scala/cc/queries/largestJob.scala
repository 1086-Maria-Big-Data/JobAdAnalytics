package cc.queries

import cc.idx.FilteredIndex.techJobTerms
import cc.idx.{CCIdxMain, FilteredIndex, IndexUtil}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, udf}
import spark.session.AppSparkSession



object largestJob extends Queries {
  def main(args: Array[String]): Unit = {
    run()
  }
  override def run(): Unit = {
    val spark = AppSparkSession()

    val df = spark.sqlContext.read
      .option("header", true)
      .schema(IndexUtil.schema)
      .csv("s3a://maria-1086/FilteredIndex/CC-MAIN-2021-**/*.csv")

    df.groupBy("url_host_name")
      .count()
      .orderBy(desc("count"))
      .show(20, false)


  }

}