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


  def main(args: Array[String]): Unit = {

    val spark = AppSparkSession()

    var list1:List[Double]=List()
    var list2:List[Double]=List()
    var list3:List[Double]=List()


    val sh=SparkHadoopUtil.get.conf
    val path = "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-21"
    val fileSystem = FileSystem.get(URI.create(path), sh)
    val it = fileSystem.listFiles(new Path(path), false)
    val status = fileSystem.listStatus(new Path(path))
    val pathList:Array[String]=status.map(x=> x.getPath.toString).filter(x=>x.contains(".csv"))
    //pathList.foreach(println)

    //for(i<- 0 to (pathList.length-1)){
    for(i<- 0 to 2){
      println(pathList(i))
      val pathToUse:String=pathList(i)
      val rdd2= WarcUtil.loadFiltered(spark.read.option("header", true).csv(path=pathToUse),enrich_payload=true)
      val xxt=rdd2.take(1000).map(x1=>SuperWarc(x1)).map{r =>r.payload(textOnly = true)}.filter{
        f=> f.matches("entry-level")|| f.matches("entry level") }
      val xxt2=xxt.filter(f=> f.contains("experience") || f.contains("Experience"))
      val xxt3=xxt2.filter(f=> !f.contains("no experience") )

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
    //dftoWrite.coalesce(1).write.format("csv").option("header", "true").mode("Overwrite").save("s3a://maria-1086/TeamQueries/entryLevel")

    spark.stop
    System.exit(0)
  }
}