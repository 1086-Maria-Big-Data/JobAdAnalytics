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
import org.apache.spark.sql.{DataFrame,Row,SparkSession}
import org.apache.hadoop.fs.{FileSystem,LocatedFileStatus,Path,RemoteIterator}

import org.archive.archivespark.functions.Html
import org.archive.archivespark.specific.warc.WarcRecord
import scala.annotation.switch
import cats.kernel.Hash

trait notSkippable {
    var _notSkippable: HashSet[String]
    def apply(word: String): Boolean = _notSkippable(word)
}

object qualificationsCertifications extends Queries {
    private val s3Path            = "s3a://maria-1086/FilteredIndex/"
    private val writePath         = "s3a://maria-1086/TeamQueries/qualifications-and-certifications/"
    private val writeDelim        = ","
    private val initialPartitions = 220
    private val crawls            = Seq(
                                    "CC-MAIN-2020-05", 
                                    "CC-MAIN-2020-10", 
                                    "CC-MAIN-2020-16", 
                                    "CC-MAIN-2020-24", 
                                    "CC-MAIN-2020-29", 
                                    "CC-MAIN-2020-34", 
                                    "CC-MAIN-2020-40", 
                                    "CC-MAIN-2020-45", 
                                    "CC-MAIN-2020-50", 
                                    "CC-MAIN-2021-04", 
                                    "CC-MAIN-2021-10", 
                                    "CC-MAIN-2021-17", 
                                    "CC-MAIN-2021-21", 
                                    "CC-MAIN-2021-25", 
                                    "CC-MAIN-2021-31"
                                )

    val notSkippable = new notSkippable {
        var _notSkippable: HashSet[String] = null
    }

    def main(args: Array[String]): Unit = {
        val spark = AppSparkSession()

        val mode = args(0)

        notSkippable._notSkippable = mode match {
            case "qualifications" => notSkippableQualifications
            case "certifications" => notSkippableCertifications
            case _ => null.asInstanceOf[HashSet[String]]
        }

        if (notSkippable == null) {
            throw new Exception(s"Invalid argument for mode: ${mode}")
            System.exit(-1)
        }

        for (crawl <- crawls) {
            val index = spark.sqlContext
                .read
                .option("header", true)
                .schema(IndexUtil.schema)
                .csv(s3Path + crawl + "/*.csv")
                .repartition(initialPartitions)
            
            val warcs = WarcUtil
                .loadFiltered(index, enrich_payload=false)
                .repartition(initialPartitions)
                .enrich(Html.first("body"))

            //Process the warc records and write to a csv file
            val fullWritePath = writePath + mode + crawl
            writeResults(processWarcRDD(warcs, spark), fullWritePath)
        }
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
        * Calls takeLines and processLine sequentially.
        *
        * @param warc the WarcRecord to process through takeLines and processLine
        * @return an Array of String-Int pairs representing a word of interest found in the
        * processLine function paired with the number 1 (in preparation for a word count)
        */
    def processWarcRecord(warc: WarcRecord): Array[(String,Int)] = {
        takeLines(SuperWarc(warc).payload)
            .filter(_.length > 0)
            .flatMap{processLine(_)}
    }

    /**
        * Takes a collected line from takeLines and parses it for qualifications of interest
        *
        * @param qLine a collected line from takeLines
        * @return an Array of String-Int pairs representing a word of interest found in the
        * processLine function paired with the number 1 (in preparation for a word count)
        */
    def processLine(qLine: String): Array[(String,Int)] = {
        qLine
            .split("(<.*?>)|:|;|\\-|(\\(.*?\\))| ")
            .foldLeft(Array[(String, Int)]()) {
                (list, word) => 
                    val transformed = removeSymbols(word).toLowerCase
                    if (notSkippableCertifications(transformed))
                        list :+ (transformed, 1)
                    else
                        list
            }
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
        IndexUtil.write(df, writeDir, writeDelim, true, 1)
    }

    private val notSkippableQualifications = HashSet(
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

    private val notSkippableCertifications = HashSet(
        "automation",
        "support",
        "comptia",
        "itf",
        "itil",
        "ccav",
        "mcse",
        "ccnp",
        "cism",
        "cissp",
        "crisc",
        "ceh",
        "pmp",
        "scrummaster",
        "cct",
        "ccna",
        "capm",
        "dasm",
        "pmi",
        "acp",
        "pmiacp",
        "dassm",
        "davsc",
        "dac",
        "csm",
        "acsm",
        "cspsm",
        "csp",
        "sm",
        "cspo",
        "csppo",
        "acsd",
        "csd",
        "cspd",
        "acap",
        "emcdsa",
        "cca",
        "cap",
        "cds",
        "opencds",
        "pcep",
        "pcap",
        "pcpp1",
        "pcpp2",
        "cepp",
        "cwp",
        "emcapd",
        "csdp",
        "mcsd",
        "mcad",
        "gweb",
        "ace",
        "cp450",
        "csslp"
    )
}

/**
  * Accesory object perform some final transformations on the data to make it more presentable.
  */
object QCResultAggregator extends App {
  val basePath = "s3a://maria-1086/TeamQueries/qualifications-and-certifications/qualifications/"
  val baseCPath = "s3a://maria-1086/TeamQueries/qualifications-and-certifications/certifications/"

  val spark = AppSparkSession()

  val smallDF = spark.read.option("header",true).option("inferSchema",true).csv(basePath + "*/*.csv").repartition(1)
  smallDF.createTempView("all")

  val smallCDF = spark.read.option("header",true).option("inferSchema",true).csv(baseCPath + "*/*.csv").repartition(1)
    smallCDF.createTempView("allc")

  val aggDF = spark.sql("SELECT * FROM (" 
            .+("SELECT 'Degree (All)' as Qualification, sum(`_2`) as `Required Count` FROM all WHERE `_1` IN('bachelors','bachelor','associates','masters','associate','phd','doctoral','doctors','doctorate', 'doctor','phds') "
            .+("UNION SELECT 'Doctors', SUM(`_2`) FROM all WHERE `_1` IN('doctoral', 'doctorate', 'doctors', 'doctor','phd','phds') "
            .+("UNION SELECT 'Masters', SUM(`_2`) FROM all WHERE `_1` = 'masters' "
            .+("UNION SELECT 'Bachelors', SUM(`_2`) FROM all WHERE `_1` IN('bachelor', 'bachelors') "
            .+("UNION SELECT 'Associates', SUM(`_2`) FROM all WHERE `_1` IN('associate', 'associates') GROUP BY `_1`) AS others "
            .+("ORDER BY `Required Count` DESC")
            ))))))

  val aggCDF = spark.sql("SELECT `_1` as Certifications, SUM(`_2`) as `Required Count` FROM allc WHERE `_1` <> 'support' GROUP BY `Certifications` ORDER BY `Required Count` DESC")

  IndexUtil.write(aggDF, "s3a://maria-1086/TeamQueries/qualifications-and-certifications/qualifications-aggregated",",",true,1)
  IndexUtil.write(aggCDF, "s3a://maria-1086/TeamQueries/qualifications-and-certifications/certifications-aggregated",",",true,1)
}