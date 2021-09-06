package cc.queries

import spark.session.AppSparkSession
import cc.warc.WarcUtil

import java.net.URI
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem,LocatedFileStatus,Path,RemoteIterator}

import org.archive.archivespark.specific.warc.WarcRecord

object Requirements extends Queries {
    private val s3Path = "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-21"

    def main(args: Array[String]): Unit = {
        run
    }

    override def run():Unit = {
        val spark = AppSparkSession()

        //Get paths to all csv files to process
        val csvPaths = getCSVPaths(s3Path)

        //Get warc records from private S3 index query results and union all shards
        val rdd2= WarcUtil.loadFiltered(spark.read.option("header", true).csv(csvPaths),enrich_payload=true)

        /**
          * Filter warc records, accumulate a count of the different
          * types of requirements, and accumulate a total count simultaneously
          */
    }

    // def functionChooser(readStartOp: String => Unit)(readContOp: String => Unit)(readStopOp: String => Unit)

    // def tentativeGaurd(readStart: => Boolean): Unit = {
    //     if (readStart)
    //         startReading
    //     else if (readAlreadyStarted)
    //         continueReading
    //     else if (readStop)
    //         addToTotalCount
    //     else
    //         lookForReadStart
    // }

    def getCSVPaths(path: String): ArrayBuffer[String] = {
        val path_ = new Path(path)
        val nodeIter = FileSystem.get(path_.toUri(), SparkHadoopUtil.get.conf).listFiles(path_,true)

        recGetCSVPaths(nodeIter)
    }

    private def recGetCSVPaths(itr: RemoteIterator[LocatedFileStatus]): ArrayBuffer[String] = {
        val files = ArrayBuffer[String]()

        while(itr.hasNext) {
            val fileOrDir       = itr.next
            val currNodePath    = fileOrDir.getPath
            val fileIsCSV       = currNodePath.toString.endWith(".csv")

            if(fileOrDir.isDirectory)
                files ++= recGetCSVPaths_(
                    FileSystem.get(currNodePath.toUri, SparkHadoopUtil.get.conf)
                    .listFiles(currNodePath,true)
                )

            if(fileOrDir.isFile && fileIsCSV)
                files += currNodePath.toString
        }
        files
    }

    def generateWarcRDD(csvPaths: ArrayBuffer[String], spark: SparkSession): RDD[WarcRecord] = {
        val warcRDDs = csvPaths.map(
            fileP => WarcUtil.loadFiltered(spark.read.option("header", true)
                .csv(fileP),false)
        )

        warcRDDs.foldLeft[RDD[WarcRecord]](Nothing)
    }

}