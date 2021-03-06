package cc.queries

import spark.session.AppSparkSession
import cc.warc.{WarcUtil,SuperWarc}
import cc.idx.IndexUtil

import java.net.URI
import scala.math.Ordering
import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.hadoop.fs.{FileSystem,LocatedFileStatus,Path,RemoteIterator}

import org.archive.archivespark.functions.Html
import org.archive.archivespark.specific.warc.WarcRecord

object qualificationsCertifications extends Queries {
    private val s3Path      = "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-21"
    private val writePath   = "s3a://maria-1086/TeamQueries/qualification-and-certifications/"
    private val writeDelim  = ","

    def main(args: Array[String]):Unit = {
        val spark = AppSparkSession()

        //Get paths to all csv files to process
        val csvPaths = getCSVPaths(s3Path)

        //Get warc records from private S3 index query results, union all shards and repartition
        val warcs = generateWarcRDD(csvPaths, spark).repartition(512)

        //Process the warc records and write to a csv file
        val fullWritePath = writePath + args(0)
        writeResults(processWarcRDD(warcs,spark),fullWritePath)
    }

    /**
      * Recurses into the given path to find all csv files
      *
      * @param path the path to search through recursively
      * @return an ArrayBuffer of type String with all csv file paths
      */
    def getCSVPaths(path: String): ArrayBuffer[String] = {
        val path_ = new Path(path)
        val nodeIter = FileSystem.get(path_.toUri(), SparkHadoopUtil.get.conf).listFiles(path_,true)

        recGetCSVPaths(nodeIter)
    }

    /**
      * Private helper function to recursively search a path for csv files
      *
      * @param itr an iterator of a remote file system
      * @return an ArrayBuffer of type String with all csv file paths
      */
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

    /**
      * Load all csv files listed in the ArrayBuffer, form a union of all formed RDDs, and
      * enrich the RDDs with the html information
      *
      * @param csvPaths ArrayBuffer containing the paths of all csv files to make into RDDs
      * @param spark a handle to the current SparkSession
      * @return an RDD containing all WARC records that the csv files make reference to
      */
    def generateWarcRDD(csvPaths: ArrayBuffer[String], spark: SparkSession): RDD[WarcRecord] = {
        val warcRDDs = csvPaths.map(
            fileP => WarcUtil.loadFiltered(spark.read.option("header", true)
                .csv(fileP),enrich_payload = false)
        )

        spark.sparkContext.union(warcRDDs).enrich(Html.first("body"))
    }

    /**
      * Searches a line for the keyword Qualification/s
      *
      * @param line the line to parse for the keyword
      * @return true if keyword is found
      */
    def findKeyWord(line: String): Boolean = {
        "[Qq](?=ualification[s]{0,1})".r.findFirstIn(line).isDefined
    }

    /**
      * After locating the keyword defined in findKeyWord, collects the
      * following numLines of lines (numLines = 20 by default) as an Array.
      * The collected lines are assumed to be the qualifications for a given job.
      *
      * @param htmlString the html payload to parse
      * @param numLines number of lines to collect after findKeyWord returns true
      * @return the collected lines
      */
    def takeLines(htmlString: String, numLines: Int=20): Array[String] = {
        val lines = htmlString
            .split("\n")
            .zipWithIndex

        val res = lines.find{
            case (line, idx) => findKeyWord(line)
        }.getOrElse(("", -1))._2

        if (res >= 0)
            return lines.slice(res, res + numLines).map(_._1)
        
        return Array[String]("")
        
    }

    /**
      * Wrapper function to improve code readability in main.
      * Calls takeLines and processQualifications sequentially.
      *
      * @param warc the WarcRecord to process through takeLines and processQualifications
      * @return an Array of String-Int pairs representing a word of interest found in the
      * processQualifications function paired with the number 1 (in preparation for a word count)
      */
    def processWarcRecord(warc: WarcRecord): Array[(String,Int)] = {
        takeLines(SuperWarc(warc).payload)
            .flatMap{processQualifications(_)}
    }

    /**
      * Takes a collected line from takeLines and parses it for qualifications of interest
      *
      * @param qLine a collected line from takeLines
      * @return an Array of String-Int pairs representing a word of interest found in the
      * processQualifications function paired with the number 1 (in preparation for a word count)
      */
    def processQualifications(qLine: String): Array[(String,Int)] = {
        qLine
            .toLowerCase
            .split("(<.*?>)|:|;|\\-|(\\(.*?\\))| ")
            .map(word => (removeSymbols(word),1))
            .filter(wn => notSkippable(wn._1))
    }

    /**
      * Removes non-alphanumeric characters
      *
      * @param word String to be parsed
      * @return String without alphanumeric characters
      */
    def removeSymbols(word: String): String = {
        word.replaceAll("\\W", "")
    }

    /**
      * Wrapper function to improve code readability in main.
      * Runs processWarcRecord on all WARC records in wRDD, counts
      * all identified words, sorts the collection in descending order,
      * and creates a DataFrame
      *
      * @param wRDD RDD of type WarcRecord to process
      * @param spark a handle to the current SparkSession
      * @return a DataFrame with the final results
      */
    def processWarcRDD(wRDD: RDD[WarcRecord], spark: SparkSession): DataFrame = {
        spark.createDataFrame(wRDD.flatMap(processWarcRecord(_)).reduceByKey(_ + _).sortBy(_._2,false))
    }

    /**
      * Writes the given DataFrame to the designated path.
      *
      * @param df the DataFrame to write
      * @param writeDir the path to write to
      */
    def writeResults(df: DataFrame, writeDir: String): Unit = {
        IndexUtil.write(df,writeDir, writeDelim, true, 1)
    }

    private val notSkippable = HashSet(
        "phd",
        "bachelor",
        "bachelors",
        "college",
        "university",
        "universitys",
        "masters",
        "phds",
        "degree",
        "doctorate",
        "associate",
        "associates",
        "certificate",
        "diploma",
        "highschool",
        "school",
        "high",
        "certification",
        "experienced",
        "doctoral",
        "doctor",
        "doctors"   
    )

}