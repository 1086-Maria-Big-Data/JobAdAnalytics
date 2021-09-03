package cc.queries

import cc.warc._
import spark.session.AppSparkSession
import org.archive.archivespark._
import org.archive.archivespark.functions._
import org.archive.archivespark.specific.warc._
import org.archive.archivespark.specific.warc.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import java.net.URI
import org.apache.spark.deploy.SparkHadoopUtil



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


    //val sh=SparkHadoopUtil.get.conf
    //val path = "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-21"
    //val fileSystem = FileSystem.get(URI.create(path), sh)
    //val it = fileSystem.listFiles(new Path(path), true)
   //while (it.hasNext()) {
      //println(it.toString)
    //}

    val pathList:List[String]=List("s3a://maria-1086/FilteredIndex/CC-MAIN-2021-21/part-00000-bd00e7f8-5888-4093-aca8-e69ea6a0deea-c000.csv")
    //val rdd2= WarcUtil.loadFiltered(spark.read.option("header", true).csv("s3a://maria-1086/FilteredIndex/CC-MAIN-2021-21/part-00000-bd00e7f8-5888-4093-aca8-e69ea6a0deea-c000.csv"),enrich_payload=true)

    for(i<- 0 to (pathList.length-1)){
      println(pathList(i))
      val rdd2= WarcUtil.loadFiltered(spark.read.option("header", true).csv(path=pathList(i)),enrich_payload=true)
      val xxt=rdd2.take(5).map(x1=>SuperWarc(x1)).map{r =>r.payload(textOnly = true)}.filter{
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