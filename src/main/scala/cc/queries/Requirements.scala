package cc.queries

import spark.session.AppSparkSession
import cc.warc.WarcUtil

import java.net.URI
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileSystem,LocatedFileStatus,Path,RemoteIterator}

import org.archive.archivespark.specific.warc.WarcRecord
import scala.collection.mutable.Stack
import scala.collection.immutable.HashMap
import scala.collection.immutable.HashSet

object Requirements extends Queries {
    // private val s3Path = "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-21"
    // private val s3Path = "s3a://maria-1086/FilteredIndex/"

    def main(args: Array[String]): Unit = {
        run
    }

    override def run():Unit = {
        val spark = AppSparkSession()

        //Get paths to all csv files to process
        // val csvPaths = getCSVPaths(s3Path)
        val csvPaths = ArrayBuffer[String]("s3a://maria-1086/FilteredIndex/CC-MAIN-2020-05/part-00000-451328f0-88e5-4cca-b2f8-703cd26ff421-c000.csv","s3a://maria-1086/FilteredIndex/CC-MAIN-2020-05/part-00001-451328f0-88e5-4cca-b2f8-703cd26ff421-c000.csv")

        //Get warc records from private S3 index query results and union all shards
        val warcs = generateWarcRDD(csvPaths, spark)

        val chArray = new Array[Byte](1024 * 5)

        warcs.zipWithIndex.filter{case (_,idx) => idx == 27702 || idx == 27703}.take(2)
            .foreach(wk => println(wk._1.data.access(is => {is.read(chArray); chArray})))
        
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
            val fileIsCSV       = currNodePath.toString.endsWith(".csv")

            if(fileOrDir.isDirectory)
                files ++= recGetCSVPaths(
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
                .csv(fileP),enrich_payload = false)
        )

        warcRDDs.reduceLeft[RDD[WarcRecord]]((f,v) => f ++ v)
    }

    def findKeyWord(line: String): Boolean = {
        "[Qq](?=ualification[s]{0,1})".r.findFirstIn(line).isDefined
    }

    // Designated space for take lines fuction

    def processQualifications(qLine: String): Seq[(String,Long)] = {
        groupAgg(
        qLine.split("<.*?>")(1).toLowerCase.split(" ")
        .foldLeft(Seq[(String,Long)]())(
            (f,v) => if (skippable(v)) f else f ++ Seq[(String,Long)]((removeExtras(v),1))
        ))
    }

    def removeExtras(word: String): String = {
        if(word.endsWith("'s") || word.endsWith("s'"))
            word.replace("'", "")
        else
            word
    }

    def skippable(word: String): Boolean = {
        ":|(|)".r.findFirstIn(word).isDefined ||
        skip(word)
    }

    def groupAgg(keyval: Seq[(String,Long)]): Seq[(String,Long)] = {
        keyval.groupBy{case (word,num) => word}
        .aggregate(Seq[(String,Long)]())(
            {case (zeros,(grpkey, kvs)) => 
                zeros ++ Seq[(String,Long)](kvs.foldLeft((grpkey,0l))
                {case ((gkey,zero),(_,num)) => (gkey,zero + num)})},
            {case (_,kvs) => kvs}
        )
    }

    private val skip = HashSet(
        "a",
        "the",
        "for",
        "in",
        "years",
        "of",
        "experience",
        "or",
        "equivalent",
        "college",
        "university",
        "accredited",
        "substituted",
        "work",
        "required"
    )
}