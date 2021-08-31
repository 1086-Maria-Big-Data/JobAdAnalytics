package cc.queries

import cc.warc._
import spark.session.AppSparkSession
import org.archive.archivespark._
import org.archive.archivespark.functions._
import org.archive.archivespark.specific.warc._
import org.archive.archivespark.specific.warc.functions._

object entryLevel extends Queries {
  def main(args: Array[String]): Unit = {
    //a very fast way to filter out all entry level not requiring experience with example for small test case

    val spark = AppSparkSession()
    //example file and in full example we bring in all paths after index specifies them
    //val rdd = WarcUtil.load("s3a://commoncrawl/crawl-data/CC-MAIN-2016-36/segments/1471982290442.1/warc/CC-MAIN-20160823195810-00000-ip-10-153-172-175.ec2.internal.warc.gz")
    val rdd=WarcUtil.load(path="s3a://commoncrawl/crawl-data/CC-MAIN-2014-23/segments/1405997885796.93/warc/CC-MAIN-20140722025805-00016-ip-10-33-131-23.ec2.internal.warc.gz")

    val countPart=rdd.getNumPartitions
    println(countPart)

    //val rdd2=rdd.repartition(numPartitions=2)
    //rdd.take(319).map(x1=>SuperWarc(x1)).foreach{r =>println(r.payload(textOnly = true).split(" ").mkString("Array(", ", ", ")"))}

    //println(rdd2.partitions.length)

   //val xx=rdd.take(300).map(x1=>SuperWarc(x1)).map{r =>(r.payload(textOnly = true).split(" ").mkString("Array(", ", ", ")").contains("Comments"))}

    //println(rdd.count)
    val xxt=rdd.enrich(HtmlText.ofEach(Html.all("body"))).toJsonStrings.take(1000).map{r=>r.split(" ").mkString("Array(", ", ", ")").contains("Comments")}

    println(xxt.count(_==true))


    //println(xx2.count(_==true))

    //val xx3=rdd.take(3).map(x1=>SuperWarc(x1)).foreach{r =>println(r.status)}

    spark.stop
    System.exit(0)
  }
}