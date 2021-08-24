package cc.idx

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.SparkSession

object IndexUtil {

    val tablePath = "s3a://commoncrawl/cc-index/table/cc-main/warc/"
    val formats = Set[String]("warc", "wat", "wet")
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

    def load(session: SparkSession): DataFrame = {
        return session.read.load(tablePath)
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

    def filter(index_df: DataFrame, crawl: String, format: String, filter_exprs: Column=null): DataFrame = {

        if (!crawls.contains(crawl)) throw new IllegalArgumentException(s"Invalid crawl ${crawl}")
        if (!formats.contains(format)) throw new IllegalArgumentException(s"Invalid format ${format}. Must be one of ${formats.mkString(", ")}")

        if (filter_exprs != null)
            return index_df.where(index_df("crawl") === crawl && index_df("subset") === format).where(filter_exprs)
        
        return index_df.where(index_df("crawl") === crawl && index_df("subset") === format)
    }

    def merge(index_dfs: DataFrame*): DataFrame = {
        return index_dfs.reduce(_ union _)
    }

    def merge(index_dfs: Iterable[DataFrame]): DataFrame = {
        return index_dfs.reduce(_ union _)
    }
}