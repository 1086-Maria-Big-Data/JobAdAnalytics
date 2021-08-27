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

    def load(session: SparkSession): DataFrame = {
        return session.read.schema(IndexUtil.schema).parquet(tablePath)
    }

    def write(df: DataFrame, path: String, include_header: Boolean=false, single_file: Boolean=true): Unit = {
        var new_df: DataFrame = null

        if (single_file) {
            new_df = df.coalesce(1)
        }
        else {
            new_df = df
        }

        if (include_header) {
            new_df.
            write.
            option("header", true).
            csv(path)
        }
        else {
            new_df.
            write.
            csv(path)
        }
    }

    def filter(index_df: DataFrame, crawl: String, subset: String, filter_exprs: Column=null): DataFrame = {

        if (!crawls.contains(crawl)) throw new IllegalArgumentException(s"Invalid crawl ${crawl}")
        if (!subsets.contains(subset)) throw new IllegalArgumentException(s"Invalid subset ${subset}. Must be one of ${subsets.mkString(", ")}")

        if (filter_exprs != null)
            return index_df.where(index_df("crawl") === crawl && index_df("subset") === subset).where(filter_exprs)
        
        return index_df.where(index_df("crawl") === crawl && index_df("subset") === subset)
    }

    def merge(index_dfs: DataFrame*): DataFrame = {
        return index_dfs.reduce(_ union _)
    }

    def merge(index_dfs: Iterable[DataFrame]): DataFrame = {
        return index_dfs.reduce(_ union _)
    }
}