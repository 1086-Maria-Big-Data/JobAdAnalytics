package cc.idx

import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, StringType, TimestampType, ShortType}

object FilterOperand extends Enumeration {
    type FilterOperand = Value

    val content_languages = Value("content_languages")

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
        StructField("subset", StringType, true)))
}