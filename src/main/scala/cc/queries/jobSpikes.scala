package cc.queries
import spark.session.AppSparkSession 
import org.apache.spark.sql.functions.col

object jobSpikes extends Queries {
    
    def main(args: Array[String]): Unit = {
        val path = "s3a://maria-1086/Testing/Mark-Testing/part-00001-bb6fa4ba-4e14-49d3-985c-e570505dc35d-c000.csv"
        jobSpikesByJob(path)
        
        def jobSpikesByJob(path:String): Unit = {
        val spark = AppSparkSession()
      
        val df = spark.read.format("csv").option("header", "true").load(path)
        df.createOrReplaceTempView("dat")
        spark.sql("SELECT (select count(url) from dat where url like '%java%') as java, (SELECT count(url) from dat where url like '%python%') as python, (SELECT count(url) from dat where url like '%scala%') as scala, (SELECT count(url) from dat where url like '%matlab%') as matlab, (SELECT count(url) from dat where url like '%SQL%') as sql from dat limit 1").show()
        
      // val df_covid_19_data = spark.read.format("csv").option("header", "true").load("input/covid_19_data.csv")
        //val rddFromFile = spark.sparkContext.textFile("s3a://maria-1086/Testing/Mark-Testing/part-00001-bb6fa4ba-4e14-49d3-985c-e570505dc35d-c000.csv",2)
        //val rdd = rddFromFile.map(f =>{f.split(",")})
        //rddFromFile.foreach(println)
        //rddFromFile.toDF()
        //val df2 = df.filter(col("url").like("%software%"))
        
        
        }
     }
}