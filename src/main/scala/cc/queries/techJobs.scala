package cc.queries

import cc.warc.WarcUtil
import org.archive.archivespark.functions.{Html, HtmlText}
import spark.session.AppSparkSession

object techJobs extends Queries {
  def main(args: Array[String]): Unit = {
    val spark = AppSparkSession()

    val rdd=WarcUtil.load(path="s3a://commoncrawl/crawl-data/CC-MAIN-2014-23/segments/1405997885796.93/warc/CC-MAIN-20140722025805-00016-ip-10-33-131-23.ec2.internal.warc.gz")
    println(rdd.take(1))

    val months = Array[String]("January","February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December")
    var monthlyPages = Array[Int](0,0,0,0,0,0,0,0,0,0,0,0)

    for (i <- 0 to 11) {

      val xxt = rdd.enrich(HtmlText.ofEach(Html.all("body"))).toJsonStrings.take(1000).map { r => r.split(" ").mkString("Array(", ", ", ")").contains(months(i)) }

      println(xxt.count(_ == true))
    }

  }
    
}