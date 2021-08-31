package cc.queries

import cc.idx.IndexUtil
import org.apache.spark.sql.functions.col
import spark.session.AppSparkSession


object largestJob extends Queries {
  def main(args: Array[String]) = {
    run()
  }
  override def run(): Unit = {
    val spark = AppSparkSession()

    IndexUtil.load(spark)
      .where(col("content_languages").rlike(".*eng.*"))
      .where(col("crawl").like("CC-MAIN-2021-31"))
      .rdd.take(1).foreach(println)
//      "s3a://maria-1086/Devin-Testing/write-test-local",
//      true, true)

  }


}