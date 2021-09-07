package cc.queries

import spark.session.AppSparkSession
import cc.warc.{WarcUtil,SuperWarc}

import java.net.URI
import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileSystem,LocatedFileStatus,Path,RemoteIterator}

import org.archive.archivespark.specific.warc.WarcRecord

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
        val totCount = spark.sparkContext.longAccumulator
        groupAgg(warcs.mapPartitions(itrWarcs => itrWarcs.flatMap(processWarcRecord(_))))
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

    def takeLines(htmlString: String, numLines: Int=10): Iterable[String] = {
        val lines = htmlString
            .split("\n")
            .zipWithIndex

        val res = lines.find{
            case (line, idx) => findKeyWord(line)
        }.getOrElse(("", -1))._2

        if (res >= 0)
            return lines.slice(res, res + numLines).map(_._1)
        
        return Seq("")
        
    }

    def processWarcRecord(warc: WarcRecord): Seq[(String,Long)] = {
        takeLines(SuperWarc(warc).payload)
            .flatMap{processQualifications(_)}
            .toSeq
    }

    def processQualifications(qLine: String): Seq[(String,Long)] = {
        qLine
            .toLowerCase
            .split("(<.*?>)|:|;|\\-|(\\(.*?\\))| ")
            .foldLeft(Seq[(String,Long)]())(
                (f,v) => if (skippable(v)) f else f ++ Seq[(String,Long)]((removeSymbols(v),1))
            )
    }

    def removeSymbols(word: String): String = {
        word.replaceAll("\\W", "")
    }

    /**
      * Not currently in use. However, it's code structure may be reused when obtaining a word count.
      *
      * @param keyval sequence of key value pairs that will be grouped by key and will have the values summed.
      * @return sequence of the sum of all values grouped by key.
      */
    def groupAgg(keyval: RDD[(String,Long)]): Seq[(String,Long)] = {
        keyval.groupByKey()

        keyval.groupBy{case (word,num) => word}
        .aggregate(Seq[(String,Long)]())(
            {case (zeros,(grpkey, kvs)) => 
                zeros ++ Seq[(String,Long)](kvs.foldLeft((grpkey,0l))
                {case ((gkey,zero),(_,num)) => (gkey,zero + num)})},
            {case (_,kvs) => kvs}
        )
    }

    private val skippable = HashSet(
        "",
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
        "required",
        "basic"
    )
}