package spark.session

import appUtil.Util

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.deploy.SparkHadoopUtil


/**Factory for SparkSession instances*/
object AppSparkSession {
    private var ss: SparkSession = null

    /**
      * Sets logging verbosity and either returns a new SparkSession instance
      * or returns a handle to the current SparkSession.
      *
      * @param log if true, sets the logging verbosity to INFO; otherwise, it is set to ERROR.
      * @return a new or existing SparkSession handle.
      */
    def apply(log: Boolean = true): SparkSession = {logging(log); getOrCreateSparkSession}

    /**
      * Sets the logging verbosity at either INFO (verbose) or ERROR.
      *
      * @param infLogLvl if true, logging verbosity is set to INFO level.
      */
    def logging(infLogLvl: Boolean): Unit = {
        val logLvl = if (infLogLvl) Level.INFO else Level.ERROR

        Logger.getLogger("org").setLevel(logLvl)
        Logger.getLogger("akka").setLevel(logLvl)
    }
    
    /**
      * Returns a configured SparkSession, constructed using createSparkSession
      */
    private def getOrCreateSparkSession: SparkSession = if (ss == null) {ss = createSparkSession; ss} else ss

    /**
      * Returns a configured SparkSession
      */
    private def createSparkSession: SparkSession = {
        val conf = new SparkConf()
            .setAppName(this.getClass.getCanonicalName())
            .set("spark.hadoop.parquet.enable.dictionary", "true")
            .set("spark.hadoop.parquet.enable.summary-metadata", "true")
            .set("spark.sql.parquet.filterPushdown", "true")
            .set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
            .set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
            .set("spark.hadoop.fs.s3a.endpoint","s3.us-east-1.amazonaws.com")
            .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryoserializer.buffer.max", "512")


         val props = Util.loadConfig()
         val access_key = props.getProperty("AWS_ACCESS_KEY_ID")
         val access_secret = props.getProperty("AWS_SECRET_ACCESS_KEY")

        System.setProperty("com.amazonaws.services.s3.enableV4", "true")

        val spark = SparkSession.builder.master("local[*]")
                .config(conf)
                .getOrCreate

        val config = spark.sparkContext.hadoopConfiguration
            config.set("fs.s3a.access.key", access_key)
            config.set("fs.s3a.secret.key", access_secret)
            config.set("fs.s3.awsAccessKeyId", access_key)
            config.set("fs.s3.awsSecretAccessKey", access_secret)            

        val sparkhadoopconfig = SparkHadoopUtil.get.conf
            sparkhadoopconfig.set("fs.s3a.access.key", access_key)
            sparkhadoopconfig.set("fs.s3a.secret.key", access_secret)
            sparkhadoopconfig.set("fs.s3.awsAccessKeyId", access_key)
            sparkhadoopconfig.set("fs.s3.awsSecretAccessKey", access_secret)

        spark
    }

}