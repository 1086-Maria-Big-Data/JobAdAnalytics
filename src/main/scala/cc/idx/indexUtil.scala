package cc.idx

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, StringType, TimestampType, ShortType}

object IndexUtil {

    val tablePath = "s3a://commoncrawl/cc-index/table/cc-main/warc/"
    val subsets = Set[String]("warc", "robotstxt", "crawldiagnostics")
    val crawls = Set[String](
        "CC-MAIN-2013-20", "CC-MAIN-2013-48", "CC-MAIN-2014-10", "CC-MAIN-2014-15", "CC-MAIN-2014-23", "CC-MAIN-2014-35", "CC-MAIN-2014-41", "CC-MAIN-2014-42", "CC-MAIN-2014-49", "CC-MAIN-2014-52", 
        "CC-MAIN-2015-06", "CC-MAIN-2015-11", "CC-MAIN-2015-14", "CC-MAIN-2015-18", "CC-MAIN-2015-22", "CC-MAIN-2015-27", "CC-MAIN-2015-32", "CC-MAIN-2015-35", "CC-MAIN-2015-40", "CC-MAIN-2015-48", 
        "CC-MAIN-2016-07", "CC-MAIN-2016-18", "CC-MAIN-2016-22", "CC-MAIN-2016-26", "CC-MAIN-2016-30", "CC-MAIN-2016-36", "CC-MAIN-2016-40", "CC-MAIN-2016-44", "CC-MAIN-2016-50", "CC-MAIN-2017-04", 
        "CC-MAIN-2017-09", "CC-MAIN-2017-13", "CC-MAIN-2017-17", "CC-MAIN-2017-22", "CC-MAIN-2017-26", "CC-MAIN-2017-30", "CC-MAIN-2017-34", "CC-MAIN-2017-39", "CC-MAIN-2017-43", "CC-MAIN-2017-47", 
        "CC-MAIN-2017-51", "CC-MAIN-2018-05", "CC-MAIN-2018-09", "CC-MAIN-2018-13", "CC-MAIN-2018-17", "CC-MAIN-2018-22", "CC-MAIN-2018-26", "CC-MAIN-2018-30", "CC-MAIN-2018-34", "CC-MAIN-2018-39", 
        "CC-MAIN-2018-43", "CC-MAIN-2018-47", "CC-MAIN-2018-51", "CC-MAIN-2019-04", "CC-MAIN-2019-09", "CC-MAIN-2019-13", "CC-MAIN-2019-18", "CC-MAIN-2019-22", "CC-MAIN-2019-26", "CC-MAIN-2019-30", 
        "CC-MAIN-2019-35", "CC-MAIN-2019-39", "CC-MAIN-2019-43", "CC-MAIN-2019-47", "CC-MAIN-2019-51", "CC-MAIN-2020-05", "CC-MAIN-2020-10", "CC-MAIN-2020-16", "CC-MAIN-2020-24", "CC-MAIN-2020-29", 
        "CC-MAIN-2020-34", "CC-MAIN-2020-40", "CC-MAIN-2020-45", "CC-MAIN-2020-50", "CC-MAIN-2021-04", "CC-MAIN-2021-10", "CC-MAIN-2021-17", "CC-MAIN-2021-21", "CC-MAIN-2021-25", "CC-MAIN-2021-31"
    )

    val schema = StructType(Array(
        StructField("url_surtkey", StringType, true),
        StructField("url", StringType, true),
        StructField("url_host_name", StringType, true),
        StructField("url_host_tld", StringType, true),
        StructField("url_host_2nd_last_part", StringType, true),
        StructField("url_host_3rd_last_part", StringType, true),
        StructField("url_host_4th_last_part", StringType, true),
        StructField("url_host_5th_last_part", StringType, true),
        StructField("url_host_registry_suffix", StringType, true),
        StructField("url_host_registered_domain", StringType, true),
        StructField("url_host_private_suffix", StringType, true),
        StructField("url_host_private_domain", StringType, true),
        StructField("url_protocol", StringType, true),
        StructField("url_port", IntegerType, true),
        StructField("url_path", StringType, true),
        StructField("url_query", StringType, true),
        StructField("fetch_time", TimestampType, true),
        StructField("fetch_status", ShortType, true),
        StructField("fetch_redirect", StringType, true),
        StructField("content_digest", StringType, true),
        StructField("content_mime_type", StringType, true),
        StructField("content_mime_detected", StringType, true),
        StructField("content_charset", StringType, true),
        StructField("content_languages", StringType, true),
        StructField("content_truncated", StringType, true),
        StructField("warc_filename", StringType, true),
        StructField("warc_record_offset", IntegerType, true),
        StructField("warc_record_length", IntegerType, true),
        StructField("warc_segment", StringType, true),
        StructField("crawl", StringType, true),
        StructField("subset", StringType, true)
    ))

    /**
     * Reads in data from parquet file into a dataframe
     *
     * Reads data from columnar parquet files to workable dataframes, which are much more easily worked with
     */
    def load(session: SparkSession): DataFrame = {
        return session.read.schema(IndexUtil.schema).parquet(tablePath)
    }

    /** Writes a DataFrame to .csv
      *
      * @param df The DataFrame to be written
      * @param path The ouput path for the .csv
      * @param delimiter The delimiter for the .csv. Default: ","
      * @param include_header Boolean determines whether to include header
      * @param num_files The number of files to write into if >= 1. Else, this argument is ignored and the original number of partitions of the DataFrame is used.
      */
    def write(df: DataFrame, path: String, delimiter: String = ",", include_header: Boolean=false, num_files: Int=0): Unit = {
        var new_df: DataFrame = null

        if (num_files >= 1) {
            new_df = df.repartition(num_files)
        }
        else {
            new_df = df
        }

        if (include_header) {
            new_df.
            write.
            option("header", true).
            option("delimiter", delimiter).
            csv(path)
        }
        else {
            new_df.
            write.
            option("delimiter", delimiter).
            csv(path)
        }
    }

    /** Writes a DataFrame to a Parquet
      * 
      * Assuming saved path="/base/path" & partition_cols=Seq("year", "month"), to read the Parquet for September 2020:
      * df = spark.sqlContext
      * .read
      * .option("basePath", "/base/path")
      * .parquet("/base/path/year=2020/fetch_month=9")
      * 
      * Similarly, to read the entire Parquet:
      * df = spark.sqlContext
      *  .read
      *  .option("basePath", "/base/path")
      *  .parquet("/base/path/")
      * 
      * @param df The DataFrame to be written
      * @param path The ouput path for the parquet
      * @param partition_cols The columns to be partitioned by. By default, no partitions are created.
      */
    def writeParquet(df: DataFrame, path: String, partition_cols: Seq[String]=null.asInstanceOf[Seq[String]]): Unit = {

        if (partition_cols == null)
            df.
            write.
            parquet(path)
        
        else
            df.
                write.
                partitionBy(partition_cols:_*).
                parquet(path)
    }

    /**
     * Filters out invalid crawls and subsets
     * 
     * Checks for IllegalArgumentExceptions and throws them out, printing out error messages for crawl and subset
     * Given a condition, the data is filtered by that condition
     * @param index_df DataFrame containing the indexes
     * @param crawl The crawl of the relevant data
     * @param subset The file type of the data being worked with
     * @param filter_exprs The parameter from which the data will be filtered with
     */
    def filter(index_df: DataFrame, crawl: String, subset: String, filter_exprs: Column=null): DataFrame = {

        if (!crawls.contains(crawl)) throw new IllegalArgumentException(s"Invalid crawl ${crawl}")
        if (!subsets.contains(subset)) throw new IllegalArgumentException(s"Invalid subset ${subset}. Must be one of ${subsets.mkString(", ")}")

        if (filter_exprs != null)
            return index_df.where(index_df("crawl") === crawl && index_df("subset") === subset).where(filter_exprs)
        
        return index_df.where(index_df("crawl") === crawl && index_df("subset") === subset)
    }
    /**
     * Takes in any number of dataframes to be combined 
     */
    def merge(index_dfs: DataFrame*): DataFrame = {
        return index_dfs.reduce(_ union _)
    }
    /**
     * Takes in datagrames wrapped in a collection to be combined
     */
    def merge(index_dfs: Iterable[DataFrame]): DataFrame = {
        return index_dfs.reduce(_ union _)
    }
}