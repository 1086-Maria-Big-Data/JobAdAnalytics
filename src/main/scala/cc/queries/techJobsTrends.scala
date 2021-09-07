package cc.queries

import cc.idx.IndexUtil
import org.apache.spark.sql.functions.{col, to_date}
import spark.session.AppSparkSession

object techJobsTrends extends Queries {
  def main(args: Array[String]): Unit = {
    val spark = AppSparkSession()
    spark.sparkContext.setLogLevel("WARN")
    val df = spark.sqlContext.read.option("header", true).schema(IndexUtil.schema).csv("s3a://maria-1086/FilteredIndex/CC-MAIN-202*-**/*.csv")

    val withDate = df.withColumn("fetch_date", to_date(col("fetch_time"),"yyyy-MM-dd"))



    val isData = withDate.withColumn("Data", col("url_path").rlike("data"))
    val isDeveloper = withDate.withColumn("Developer", col("url_path").rlike("developer"))
    val isWeb = withDate.withColumn("Web", col("url_path").rlike("web"))

    val groupedIsData = isData.filter(col("Data")===true).groupBy("fetch_date").count().orderBy("fetch_date")

    val groupedIsDeveloper = isDeveloper.filter(col("Developer")===true).groupBy("fetch_date").count().orderBy("fetch_date")

    val groupedIsWeb = isWeb.filter(col("Web")===true).groupBy("fetch_date").count().orderBy("fetch_date")

    val grouped = withDate.groupBy("fetch_date").count().orderBy("fetch_date")

    IndexUtil.write(grouped, "s3a://maria-1086/TeamQueries/tech-job-posting-trends/totalTechJobs", include_header=true, num_files=1)
    IndexUtil.write(groupedIsData, "s3a://maria-1086/TeamQueries/tech-job-posting-trends/dataJobs", include_header=true, num_files=1)
    IndexUtil.write(groupedIsDeveloper, "s3a://maria-1086/TeamQueries/tech-job-posting-trends/developerJobs", include_header=true, num_files=1)
    IndexUtil.write(groupedIsWeb, "s3a://maria-1086/TeamQueries/tech-job-posting-trends/webJobs", include_header=true, num_files=1)

  }
    
}