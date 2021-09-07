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
import org.apache.spark.util.AccumulatorV2

object qualificationsCertifications extends Queries {
    private val s3Path      = "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-21"
    private val writePath   = "s3a://maria-1086/TeamQueries/qualification-and-certifications/"
    private val writeDelim  = ","

    def main(args: Array[String]):Unit = {
        val spark   = AppSparkSession()
        val wordAcc = new MapAccumulatorV2()

        spark.sparkContext.register(wordAcc, "wcMapAccumulator")

        //Get warc records from private S3 index query results, union all shards and repartition
        val testPath = "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-21/part-00000-bd00e7f8-5888-4093-aca8-e69ea6a0deea-c000.csv"
        val warcs = generateWarcRDD(s3Path, spark).repartition(512)

        //Process the warc records and write to a csv file
        processWarcRDD(warcs,spark,wordAcc)

        wordAcc.value.foreach{case (word, count) => println(word + "\t:: " + count)}
        // val fullWritePath = writePath + args(0)
        // writeResults(wordAcc,spark,fullWritePath)
    }

    /**
      * Load all csv files found in csvBasePath and
      * enrich the RDD with the html information
      *
      * @param csvBasePath path containing all csv files to make into RDDs
      * @param spark a handle to the current SparkSession
      * @return an RDD containing all WARC records that the csv files make reference to
      */
    def generateWarcRDD(csvBasePath: String, spark: SparkSession): RDD[WarcRecord] = {
        WarcUtil.loadFiltered(spark.read.option("header", true)
                .csv(csvBasePath /*+ "/*.csv"*/*/),enrich_payload = false)
                .enrich(Html.first("body"))
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
    def processWarcRecord(warc: WarcRecord, macc: MapAccumulatorV2): Unit = {
        takeLines(SuperWarc(warc).payload)
                    .foreach{macc.add("Total Records"); processQualifications(_,macc)}
    }

    /**
      * Takes a collected line from takeLines and parses it for qualifications of interest
      *
      * @param qLine a collected line from takeLines
      * @return an Array of String-Int pairs representing a word of interest found in the
      * processQualifications function paired with the number 1 (in preparation for a word count)
      */
    def processQualifications(qLine: String, macc: MapAccumulatorV2): Unit = {
        qLine
            .toLowerCase
            .split("(<.*?>)|(\\(.*?\\))| ")
            .foldLeft(())((f,v) => {
              val fmtWord = removeSymbols(v)

              if(notSkippable(fmtWord))
                macc.add(fmtWord)
              else
                f
            })
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
    def processWarcRDD(wRDD: RDD[WarcRecord], spark: SparkSession, macc: MapAccumulatorV2): Unit = {
            wRDD.foreach(processWarcRecord(_,macc))
    }

    /**
      * Writes the given DataFrame to the designated path.
      *
      * @param df the DataFrame to write
      * @param writeDir the path to write to
      */
    def writeResults(macc: MapAccumulatorV2, spark: SparkSession, writeDir: String): Unit = {
        val df = spark.createDataFrame(macc.value.toList)

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

    private class MapAccumulatorV2 extends org.apache.spark.util.AccumulatorV2[String, Map[String,Int]] {
      private val accumap = scala.collection.mutable.Map[String,Int]()

      override def add(v: String): Unit = {
        if(accumap.contains(v))
          accumap(v) += 1
        else
          accumap(v) = 1
        
      }

      override def copy(): AccumulatorV2[String,Map[String,Int]] = {
        val cp = new MapAccumulatorV2()
        cp.mergeValues(value)
        cp
      }

      override def isZero: Boolean = {
        accumap.isEmpty
      }

      override def reset(): Unit = {
        accumap.empty
      }
      
      override def merge(other: AccumulatorV2[String,Map[String,Int]]): Unit = {
        mergeValues(other.value)
      }
      
      override def value: Map[String,Int] = {
        accumap.toMap
      }

      private def mergeValues(other: Map[String,Int]): Unit = {
        for ((key,value) <- other)
          if(accumap.contains(key))
            accumap(key) += value
          else 
            accumap(key) = value
      }
    } 
}

