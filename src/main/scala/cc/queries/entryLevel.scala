package cc.queries

import cc.warc._
import spark.session.AppSparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI
import org.apache.spark.deploy.SparkHadoopUtil



object entryLevel extends Queries {

  def main(args: Array[String]): Unit = {
   //xxt=rdd2.take(100)) can be changed to desired number and output file should have correct location at bottom
    val spark = AppSparkSession()

    var list1:List[Double]=List()
    var list2:List[Double]=List()
    var list3:List[Double]=List()

    val firstPathList:Array[String]=Array("CC-MAIN-2020-05","CC-MAIN-2020-10","CC-MAIN-2020-16","CC-MAIN-2020-24","CC-MAIN-2020-29","CC-MAIN-2020-34",
    "CC-MAIN-2020-40","CC-MAIN-2020-45","CC-MAIN-2020-50","CC-MAIN-2021-04","CC-MAIN-2021-10","CC-MAIN-2021-17","CC-MAIN-2021-21","CC-MAIN-2021-25",
    "CC-MAIN-2021-31")

    //for(j<-0 to (firstPathList.length-1)) {
    for(j<-(firstPathList.length-1) to (firstPathList.length-1)){

      val sh = SparkHadoopUtil.get.conf
      //val path = "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-21"
      val path="s3a://maria-1086/FilteredIndex/"++firstPathList(j)
      val fileSystem = FileSystem.get(URI.create(path), sh)
      val it = fileSystem.listFiles(new Path(path), false)
      val status = fileSystem.listStatus(new Path(path))
      val pathList: Array[String] = status.map(x => x.getPath.toString).filter(x => x.contains(".csv"))
      //pathList.foreach(println)

      for (i <- 0 to (pathList.length - 1)) {
        //for(i<- 0 to 1){
        println(pathList(i))
        println(i)
        val pathToUse: String = pathList(i)
        val rdd2 = WarcUtil.loadFiltered(spark.read.option("header", true).csv(path = pathToUse), enrich_payload = true)
        val xxt = rdd2.take(100).map(x1 => SuperWarc(x1)).map { r => r.payload(textOnly = true) }.filter {
          f => f.matches(".*[Ee]ntry-[Ll]evel.*") || f.matches(".*[Ee]ntry [Ll]evel.*")
        }
        val xxt2 = xxt.filter(f => f.matches(".*[Ee]xperience.*"))
        val xxt3 = xxt2.filter(f => !f.matches(".*[Nn]o [Ee]xperience.*"))

        list1 = list1 ++ List(xxt.length.toDouble)
        list2 = list2 ++ List(xxt3.length.toDouble)
        list3 = list3 ++ List((xxt3.length.toDouble / xxt.length.toDouble) * 100)

        println(xxt.length.toString ++ "," ++ xxt3.length.toString ++ "," ++ (xxt3.length.toDouble / xxt.length.toDouble).toString)

      }
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