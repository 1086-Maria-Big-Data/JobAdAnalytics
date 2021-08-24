package cc.idx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession

object CCIdxMain {
    def main(args: Array[String]): Unit = {
		val tablePath = "s3a://commoncrawl/cc-index/table/cc-main/warc/"
		val viewName = "ccindex"
			val sqlQuery = "Select * From " + viewName + " Where crawl=\'CC-MAIN-2021-10\' And subset=\'warc\' AND url RLIKE \'.*(/job/|/jobs/|/careers/|/career/).*\'"


			val conf = new SparkConf().setAppName(this.getClass.getCanonicalName())
			val spark = SparkSession.builder.master("local[*]").config(conf).getOrCreate
			val sc = spark.sparkContext
		val df = spark.read.load(tablePath)
		df.createOrReplaceTempView(viewName)
		spark.sql(sqlQuery).show

		spark.stop

        System.exit(0)
    }
}