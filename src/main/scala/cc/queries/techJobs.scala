package cc.queries

import cc.idx.IndexUtil
import cc.warc.WarcUtil
import org.apache.spark.sql.functions.{col, to_date}
import org.archive.archivespark.functions.{Html, HtmlText}
import org.archive.archivespark.sparkling.cdx.CdxRecord
import spark.session.AppSparkSession

object techJobs extends Queries {
  def main(args: Array[String]): Unit = {
    val spark = AppSparkSession()
    val df = spark.read.csv("s3a://maria-1086/Devin-Testing/outputs/test-write/")

    val withDate = df.withColumn("fetch_date", to_date(col("_c1"),"yyyy-MM-dd"))

    val grouped = withDate.groupBy("fetch_date").count().orderBy("fetch_date")

    grouped.show()
    //withDate.printSchema()
    //withDate.show(1)

  }
    
}