package cc.queries

import cc.idx.CCIdxMain.tablePath
import cc.idx.IndexUtil
import spark.session.AppSparkSession

object largestJob extends Queries {
  val crawls:Set[String] = Set[String]("CC-MAIN-2018-05", "CC-MAIN-2018-09", "CC-MAIN-2018-13", "CC-MAIN-2018-17", "CC-MAIN-2018-22", "CC-MAIN-2018-26", "CC-MAIN-2018-30", "CC-MAIN-2018-34", "CC-MAIN-2018-39",
  "CC-MAIN-2018-43", "CC-MAIN-2018-47", "CC-MAIN-2018-51", "CC-MAIN-2019-04", "CC-MAIN-2019-09", "CC-MAIN-2019-13", "CC-MAIN-2019-18", "CC-MAIN-2019-22", "CC-MAIN-2019-26", "CC-MAIN-2019-30",
  "CC-MAIN-2019-35", "CC-MAIN-2019-39", "CC-MAIN-2019-43", "CC-MAIN-2019-47", "CC-MAIN-2019-51", "CC-MAIN-2020-05", "CC-MAIN-2020-10", "CC-MAIN-2020-16", "CC-MAIN-2020-24", "CC-MAIN-2020-29",
  "CC-MAIN-2020-34", "CC-MAIN-2020-40", "CC-MAIN-2020-45", "CC-MAIN-2020-50", "CC-MAIN-2021-04", "CC-MAIN-2021-10", "CC-MAIN-2021-17", "CC-MAIN-2021-21", "CC-MAIN-2021-25", "CC-MAIN-2021-31"
  )

  val spark = AppSparkSession()

  def main(args: Array[String]): Unit = {
    run()
  }
  override def run(): Unit = {
    val df = spark.read.schema(IndexUtil.schema).parquet(IndexUtil.tablePath)

    val queriedDF = IndexUtil.merge(crawls.map(crawl => IndexUtil.filter(df, crawl, "warc", df.col("content_languages").contains("eng"))))

    queriedDF.select("warc_filename", "warc_record_offset","warc_record_length", "url").show(10, false)
  }


}