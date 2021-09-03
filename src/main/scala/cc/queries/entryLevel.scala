package cc.queries

import cc.warc._
import spark.session.AppSparkSession
import org.archive.archivespark._
import org.archive.archivespark.functions._
import org.archive.archivespark.specific.warc._
import org.archive.archivespark.specific.warc.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._



object entryLevel extends Queries {

  //may need to adjust following especicially on local for optimization:
  //config.set("fs.s3a.multipart.size", "100")
  // config.set("fs.s3a.threads.core", "10")
  // config.set("fs.s3a.block.size", "32") in AppSpark Session also this may fix Amazon Jave Heap error too
  //does not hang on Amazon though

  //still need to do some optimizations this version is easiest to run on AWS, others take awhile

  def main(args: Array[String]): Unit = {

    //a very fast way to filter out all entry level not requiring experience with example for one file test case
    val spark = AppSparkSession()
    var list1:List[Double]=List()
    var list2:List[Double]=List()
    var list3:List[Double]=List()
    //100 example WARC files used
    //val df=spark.read.csv(path="s3://maria-1086/Testing/will_testing/part100.csv")
    //val df = spark.read.csv("input/part100.csv")
    //val ArrayFilesPaths=df.collect.map(r=>r.toString)
    //println(ArrayFilesPaths.length)
    //ArrayFilesPaths.foreach(println)
    //in this example we use two files
    //remove "[" from file and add on "s3a://commoncrawl/" and take only first 2, set second slice number for
    //file number from part100.csv if using from file or uncomment s3 with file to use on Amazon
    //uncomment file if using from file but can just use below for quick test
    val IndexListTStr:Array[String]=Array("s3a://commoncrawl/crawl-data/CC-MAIN-2016-36/segments/1471982290442.1/warc/CC-MAIN-20160823195810-00000-ip-10-153-172-175.ec2.internal.warc.gz",
      "s3a://commoncrawl/crawl-data/CC-MAIN-2014-23/segments/1405997885796.93/warc/CC-MAIN-20140722025805-00016-ip-10-33-131-23.ec2.internal.warc.gz")
    //val IndexListTStr:Array[String]=ArrayFilesPaths.slice(0,2).map(r=>"s3a://commoncrawl/"++r.slice(1,r.length-1))
    println(IndexListTStr.length)

    for(i<- 0 to (IndexListTStr.length-1)){
      println(IndexListTStr(i))
      val rdd1:RDD[WarcRecord]=WarcUtil.load(IndexListTStr(i))
      val xxt=rdd1.enrich(HtmlText.ofEach(Html.all("body"))).toJsonStrings.take(10000).filter{
        f=> f.contains("entry-level")|| f.contains("entry level")}
      val xxt2=xxt.filter(f=> f.contains("experience"))
      val xxt3=xxt2.filter(f=> !f.contains("no experience"))

      list1=list1++List(xxt.length.toDouble)
      list2=list2++List(xxt3.length.toDouble)
      list3=list3++List((xxt3.length.toDouble/xxt.length.toDouble)*100)

      println(xxt.length.toString++","++ xxt3.length.toString++","++(xxt3.length.toDouble/xxt.length.toDouble).toString)

    }
    val Total=(list1,list2,list3).zipped.toList
    val dftoWrite = spark.createDataFrame(Total).toDF("Total Entry-Level Tech Jobs", "Entry-Level Exp. Req.","Percent Requiring Exp.")
    dftoWrite.show()
    dftoWrite.coalesce(1).write.format("csv").option("header","true").mode("Overwrite").save("/output/testing")
    //dftoWrite.coalesce(1).write.format("csv").option("header", "true").mode("Overwrite").save("s3a://maria-1086/Testing/will_testing/newresults")

    spark.stop
    System.exit(0)
  }
}