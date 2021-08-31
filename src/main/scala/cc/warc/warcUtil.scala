package cc.warc

import org.archive.archivespark._
import org.archive.archivespark.functions._
import org.archive.archivespark.specific.warc._
import org.archive.archivespark.specific.warc.functions._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, IntegerType, LongType}

import org.archive.archivespark.sparkling.cdx.CdxRecord

/**
  * Utility object for WARC loading methods
  */
object WarcUtil {

    val titleTextEnricher = HtmlText.of(Html.first("title"))
    val bodyTextEnricher = HtmlText.of(Html.first("body"))

    implicit val enc = Encoders.product[CdxRecord]
    /** Loads an s3 object from the specified `path` into an `RDD[WarcRecord]`
      * 
      *
      * @param path - a `String` to the path of the object
      * @param enrich_payload - optional `Boolean` parameter to enrich the text of the payload to remove html tags
      * @return `RDD[WarcRecord]`
      */
    def load(path: String, enrich_payload: Boolean=true): RDD[WarcRecord] = {
        if (enrich_payload)
            return ArchiveSpark
                .load(WarcSpec.fromFiles(path))
                .enrich(WarcPayload)
                .enrich(titleTextEnricher)
                .enrich(bodyTextEnricher)

        return ArchiveSpark
            .load(WarcSpec.fromFiles(path))
    }
    /** Returns an `RDD[WarcRecord]` from a filtered `DataFrame`
      * 
      *
      * @param filtered_df - a presumed filtered `DataFrame`
      * @param root_path - optional `String` path for cdxWarcPaths.  Default = "s3a://commoncrawl/"
      * @param enrich_payload - optional `Boolean` to return enriched payload.  Default = "true"
      * @return `RDD[WarcRecord]` containing warcs from the filtered `DataFrame`
      */
    def loadFiltered(filtered_df: DataFrame, root_path: String="s3a://commoncrawl/", enrich_payload: Boolean=true): RDD[WarcRecord] = {

        val rdd_cdx = filtered_df
            .withColumn("meta", lit("-"))
            .withColumn("additionalFields", array(col("warc_record_offset").cast(StringType), col("warc_filename")))
            .select(
                col("url_surtkey").as("surtUrl"), 
                col("fetch_time").as("timestamp").cast(StringType), 
                col("url").as("originalUrl"), 
                col("content_mime_type").as("mime"), 
                col("fetch_status").as("status").cast(IntegerType), 
                col("content_digest").as("digest"), 
                col("fetch_redirect").as("redirectUrl"), 
                col("meta"),
                col("warc_record_length").as("compressedSize").cast(LongType), 
                col("additionalFields"))
            .as[CdxRecord]
            .rdd
            .map((_, root_path))

        if (enrich_payload)
            return ArchiveSpark
                .load(WarcSpec.fromFiles(rdd_cdx))
                .enrich(WarcPayload)
                .enrich(cc.warc.WarcUtil.titleTextEnricher)
                .enrich(cc.warc.WarcUtil.bodyTextEnricher)

        return ArchiveSpark
            .load(WarcSpec.fromFiles(rdd_cdx))
    }
}

object SuperWarc {
    /**
      * Constructor for SuperWarc wrapper class
      *
      * @param record - `WarcRecord`
      * @return - `SuperWarc`
      */
    def apply(record: WarcRecord): SuperWarc = {
        return new SuperWarc(record)
    }
}
/**
  *   A SuperWarc.
  *   An ArchiveSpark.WarcRecord wrapper class that provides high utility for accessing its many attributes.
  *
  * @param _record - `WarcRecord`
  */
class SuperWarc private (_record: WarcRecord) {
    /**
      *   recordHeader (Map[String, String]): The WARC record header containing the metadata of its creation.
      *       WARC-Type: (e.g. "response")
      *       WARC-Date: (e.g. "2021-08-05T21:13:24Z")
      *       WARC-Record-ID: (e.g. "<urn:uuid:5322719c-d2c7-418b-8cfb-52873ea71018>")
      *       Content-Length: (e.g. "92438")
      *       Content-Type: (e.g. "application/http; msgtype=response")
      *       WARC-Warcinfo-ID: (e.g. "<urn:uuid:f7de1d8f-6921-461e-a1e3-1ba2f39d8645>")
      *       WARC-Concurrent-To: (e.g. "<urn:uuid:a79c4005-5108-45b0-95d3-034f8c187f3a>")
      *       WARC-IP-Address: (e.g. "45.58.174.238")
      *       WARC-Target-URI: (e.g. "http://072262.17tao.win/")
      *       WARC-Payload-Digest: (e.g. "sha1:L6BFS5APE4HTFNP3YURO4KUIC3HZ23PW")
      *       WARC-Block-Digest: (e.g. "sha1:TVCWX7ROQG3QCJMO4NDIL4PF7TJQBR5K")
      *       WARC-Identified-Payload-Type: (e.g. "text/html")
      */
    private lazy val _recordHeader: Map[String, String] = _record.get[Map[String, String]]("recordHeader") match {
        case Some(i) => i.asInstanceOf[Map[String, String]]
        case None => Map[String, String]()
    }
    /**
      *     httpStatusLine (String): The Status Line of the HTTP response header. (e.g. HTTP/1.1 200 OK)
      */
    private lazy val _httpStatusLine: String = _record.get[String]("httpStatusLine") match {
        case Some(i) => i.asInstanceOf[String]
        case None => ""
    }
    /**
      *     httpHeader (Map[String, String]): The HTTP response header fields.
      *       Server: (e.g. "Tengine")
      *       Date: (e.g. "Thu, 05 Aug 2021 21:13:24 GMT")
      *       Content-Type: (e.g. "text/html; charset=UTF-8")
      *       X-Crawler-Transfer-Encoding: (e.g. "chunked")
      *       Connection: (e.g. "keep-alive")
      *       Vary: (e.g. "Accept-Encoding")
      *       X-Crawler-Content-Encoding: (e.g. "gzip")
      *       Content-Length: (e.g. "92183")
      */
    private lazy val _httpHeader: Map[String, String] = _record.get[Map[String, String]]("httpHeader") match {
        case Some(i) => i.asInstanceOf[Map[String, String]]
        case None => Map[String, String]()
    }
    /**
      *       payload (String): The payload of the HTTP response body of the WARC record. Available in two modes.
      *       a) Raw: The complete HTML of the original webpage.
      *       b) Text-Only: The text-only version of the webpage. All HTML tags are removed.
      */
    private lazy val _payload: String = _record.get[Array[Byte]]("payload") match {
        case Some(i) => i.asInstanceOf[Array[Byte]].map(_.toChar).mkString
        case None => ""
    }
    /**
      * Private method returns html text without the tags
      *
      * @return - `String` of html text
      */
    private def parsePayload(): String = {
        return _record.valueOrElse(WarcUtil.titleTextEnricher, "") + _record.valueOrElse(WarcUtil.bodyTextEnricher, "")
    }
    /**
      * Returns original `WarcRecord`
      *
      * @return - `WarcRecord`
      */
    def get(): WarcRecord = _record
    /**
      * originalUrl `String` : Full URL of the WARC record.
      *
      * @return `String`
      */
    def originalUrl(): String = _record.originalUrl
    /**
      * mime `String`: MIME format. Usually "text/html".
      *
      * @return `String`
      */
    def mime(): String = _record.mime
    /**
      * status `Int`: HTTP response status codes. (e.g. 200 OK, 400 BAD REQUEST, 404 NOT FOUND etc.)
      *
      * @return `Int`
      */
    def status(): Int = _record.status
    /**
      * toCdxString `String`: The CDX representation of this WARC record.
      *
      * @return `String`
      */
    def toCdxString(): String = _record.toCdxString
    /**
      * toCdxString `String`: The CDX representawtion of this WARC record with addtional fields
      *
      * @param includeAdditionalFields - `Boolean`
      * @return
      */
    def toCdxString(includeAdditionalFields: Boolean=true): String = _record.toCdxString(includeAdditionalFields)
    /**
      * toJsonString `String`: The JSON representation of this WarcRecord Object.
      *
      * @return `String`
      */
    def toJsonString(): String = _record.toJsonString
    /**
      * toJsonString `String`: The JSON representation of this WarcRecord Object with 4-space indentations
      *
      * @param pretty - `Boolean` default = true
      * @return `String`
      */
    def toJsonString(pretty: Boolean=true): String = _record.toJsonString(pretty)
    /**
      * Returns the WarcRecord header
      *
      * @return `Map[String, String]`
      */
    def recordHeader(): Map[String, String] = _recordHeader
    /**
      * Returns a specific field from the WarcRecord header
      *
      * @param field - `String` name of the field to be returned
      * @return `String`
      */
    def recordHeader(field: String): String = _recordHeader(field)
    /**
      * Returns the httpStatusLine `String`: The Status Line of the HTTP response header. (e.g. HTTP/1.1 200 OK)
      *
      * @return `String`
      */
    def httpStatusLine(): String = _httpStatusLine
    /**
      * Returns httpHeader `Map[String, String]`: The HTTP response header fields.
      *
      * @return `Map[String, String]`
      */
    def httpHeader(): Map[String, String] = _httpHeader
    /**
      * Returns a specific field `String` from the httpHeader
      *
      * @param field - `String` name of the field to be returned
      * @return `String`
      */
    def httpHeader(field: String): String = _httpHeader(field)
    /**
      * payload `String` : Returns the complete HTML of the original webpage from the payload of the WARC record.
      *
      * @return `String`
      */
    def payload(): String = _payload
    /**
      * payload `String` : Returns the text-only version of the webpage from the payload of the WARC record. All HTML tags are removed.
      *
      * @param textOnly - `Boolean` default = true
      * @return `String`
      */
    def payload(textOnly: Boolean=true): String = if (textOnly) parsePayload() else _payload
}