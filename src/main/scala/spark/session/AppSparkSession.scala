package spark.session

import appUtil.Util

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.deploy.SparkHadoopUtil
import _root_.org.apache.hadoop.fs.FileSystem


object AppSparkSession {
    private var ss: SparkSession = null

    def apply(): SparkSession = if (ss == null) {ss = createSparkSession; ss} else ss

    private def createSparkSession: SparkSession = {
        // Logger.getLogger("org").setLevel(Level.ERROR)
        // Logger.getLogger("akka").setLevel(Level.ERROR)

        val conf = new SparkConf()
            .setAppName(this.getClass.getCanonicalName())
            .set("spark.hadoop.parquet.enable.dictionary", "true")
            .set("spark.hadoop.parquet.enable.summary-metadata", "true")
            .set("spark.sql.hive.metastorePartitionPruning", "true")
            .set("spark.sql.parquet.filterPushdown", "true")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.executor.userClassPathFirst", "true")

        val props = Util.loadConfig()
        val access_key = props.getProperty("AWS_ACCESS_KEY_ID")
        val access_secret = props.getProperty("AWS_SECRET_ACCESS_KEY")

        val spark = SparkSession.builder.master("local[*]")
                .config(conf)
                .getOrCreate

        val config = spark.sparkContext.hadoopConfiguration
            config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
            config.set("fs.s3a.awsAccessKeyId", access_key)
            config.set("fs.s3a.awsSecretAccessKey", access_secret)
            config.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
            config.set("fs.s3.awsAccessKeyId", access_key)
            config.set("fs.s3.awsSecretAccessKey", access_secret)
            config.set("fs.defaultFS", "s3a://commoncrawl/")

        val sparkhadoopconfig = SparkHadoopUtil.get.conf
            sparkhadoopconfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
            sparkhadoopconfig.set("fs.s3a.awsAccessKeyId", access_key)
            sparkhadoopconfig.set("fs.s3a.awsSecretAccessKey", access_secret)
            sparkhadoopconfig.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
            sparkhadoopconfig.set("fs.s3.awsAccessKeyId", access_key)
            sparkhadoopconfig.set("fs.s3.awsSecretAccessKey", access_secret)
            sparkhadoopconfig.set("fs.defaultFS", "s3a://commoncrawl/")

        spark
    }
}