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

object WarcUtil {

    val titleTextEnricher = HtmlText.of(Html.first("title"))
    val bodyTextEnricher = HtmlText.of(Html.first("body"))

    implicit val enc = Encoders.product[CdxRecord]

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
    def apply(record: WarcRecord): SuperWarc = {
        return new SuperWarc(record)
    }
}

class SuperWarc private (_record: WarcRecord) {

    /*
    *   A SuperWarc.
    *   An ArchiveSpark.WarcRecord wrapper class that provides high utility for accessing its many attributes.
    * 
    * 
    * 
    *   Attributes:
    *   
    *   originalUrl (String) : Full URL of the WARC record.
    *   
    *   mime (String): MIME format. Usually "text/html".
    * 
    *   status (Int): HTTP response status codes. (e.g. 200 OK, 400 BAD REQUEST, 404 NOT FOUND etc.)
    * 
    *   toCdxString (String): The CDX representation of this WARC record.
    * 
    *   toJsonString (String): The JSON representation of this WarcRecord Object.
    * 
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
    * 
    *   httpStatusLine (String): The Status Line of the HTTP response header. (e.g. HTTP/1.1 200 OK)
    * 
    *   httpHeader (Map[String, String]): The HTTP response header fields.
    *       Server: (e.g. "Tengine")
    *       Date: (e.g. "Thu, 05 Aug 2021 21:13:24 GMT")
    *       Content-Type: (e.g. "text/html; charset=UTF-8")
    *       X-Crawler-Transfer-Encoding: (e.g. "chunked")
    *       Connection: (e.g. "keep-alive")
    *       Vary: (e.g. "Accept-Encoding")
    *       X-Crawler-Content-Encoding: (e.g. "gzip")
    *       Content-Length: (e.g. "92183")
    * 
    *   payload (String): The payload of the HTTP response body of the WARC record. Available in two modes.
    *       a) Raw: The complete HTML of the original webpage.
    *       b) Text-Only: The text-only version of the webpage. All HTML tags are removed.
    * 
    */

    private lazy val _recordHeader: Map[String, String] = _record.get[Map[String, String]]("recordHeader") match {
        case Some(i) => i.asInstanceOf[Map[String, String]]
        case None => Map[String, String]()
    }

    private lazy val _httpStatusLine: String = _record.get[String]("httpStatusLine") match {
        case Some(i) => i.asInstanceOf[String]
        case None => ""
    }

    private lazy val _httpHeader: Map[String, String] = _record.get[Map[String, String]]("httpHeader") match {
        case Some(i) => i.asInstanceOf[Map[String, String]]
        case None => Map[String, String]()
    }

    private lazy val _payload: String = _record.get[Array[Byte]]("payload") match {
        case Some(i) => i.asInstanceOf[Array[Byte]].map(_.toChar).mkString
        case None => ""
    }

    private def parsePayload(): String = {
        return _record.valueOrElse(WarcUtil.titleTextEnricher, "") + _record.valueOrElse(WarcUtil.bodyTextEnricher, "")
    }
    
    def get(): WarcRecord = _record

    def originalUrl(): String = _record.originalUrl
    def mime(): String = _record.mime
    def status(): Int = _record.status
    def toCdxString(): String = _record.toCdxString
    def toCdxString(includeAdditionalFields: Boolean=true): String = _record.toCdxString(includeAdditionalFields)
    def toJsonString(): String = _record.toJsonString
    def toJsonString(pretty: Boolean=true): String = _record.toJsonString(pretty)

    def recordHeader(): Map[String, String] = _recordHeader
    def recordHeader(field: String): String = _recordHeader(field)

    def httpStatusLine(): String = _httpStatusLine

    def httpHeader(): Map[String, String] = _httpHeader
    def httpHeader(field: String): String = _httpHeader(field)

    def payload(): String = _payload
    def payload(textOnly: Boolean=true): String = if (textOnly) parsePayload() else _payload
}