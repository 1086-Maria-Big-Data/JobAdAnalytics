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

    //val warc_rdd = WarcUtil.loadFiltered(withDate)

    val isData = withDate.withColumn("Data", col("url_path").rlike("data"))
    val isDeveloper = withDate.withColumn("Developer", col("url_path").rlike("developer"))
    val isWeb = withDate.withColumn("Web", col("url_path").rlike("web"))

    val groupedIsData = isData.filter(col("Data")===true).groupBy("fetch_date").count().orderBy("fetch_date")

    val groupedIsDeveloper = isDeveloper.filter(col("Developer")===true).groupBy("fetch_date").count().orderBy("fetch_date")

    val groupedIsWeb = isWeb.filter(col("Web")===true).groupBy("fetch_date").count().orderBy("fetch_date")

    val grouped = withDate.groupBy("fetch_date").count().orderBy("fetch_date")
  /* Code for going into payload
    val warc_df = warc_rdd
      .map(warc => SuperWarc(warc))
      .map(warc => (warc.recordHeader("WARC-Date").substring(0,10), warc.payload(true).split(" ")))
      .flatMap(record => record._2.map(x => (record._1, x, 1)))
      .map { case (date, word, count) => ((date, word), count)}.reduceByKey(_+_)
      // val initialSet = mutable.HashSet.empty[(Int, Int)]
      // val addToSet = (s: mutable.HashSet[(Int, Int)], v: (Int,Int)) => s += v
      // val mergePartitionSets = (p1: mutable.HashSet[(Int, Int)], p2: mutable.HashSet[(Int, Int)]) => p1 ++= p2
      // val uniqueByKey = warc_df.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
      .map { case ((date, word), count) => (date, word, count) }
    //.take(5)
    //.groupBy(_._1)
    //.map(record => (record._1, record._2.map(_._1).reduce(_+_), record._2.map(_._3).reduce(_ + _)))
    //.toSeq
    val wordCount_df = spark.createDataFrame(warc_df).toDF("date", "word", "count")

    val ordered = wordCount_df.orderBy("word")

    ordered.show(50)


   */
    //println("Data Jobs")
    //groupedIsData.show()
    //println("Developer Jobs")
    //groupedIsDeveloper.show()
    //println("Web Jobs")
    //groupedIsWeb.show()
    //println("Total Tech Jobs")
    //grouped.show()
    IndexUtil.write(grouped, "s3a://maria-1086/Testing/Brian_Testing/techJobs/tTechJobs", include_header=true, num_files=1)
    IndexUtil.write(groupedIsData, "s3a://maria-1086/Testing/Brian_Testing/techJobs/dataJobs", include_header=true, num_files=1)
    IndexUtil.write(groupedIsDeveloper, "s3a://maria-1086/Testing/Brian_Testing/techJobs/developerJobs", include_header=true, num_files=1)
    IndexUtil.write(groupedIsWeb, "s3a://maria-1086/Testing/Brian_Testing/techJobs/webJobs", include_header=true, num_files=1)

  }
    
}