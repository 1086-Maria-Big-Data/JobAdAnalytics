package cc.idx

import cc.warc.WarcUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.archive.archivespark.specific.warc.WarcRecord
import spark.session.AppSparkSession
import scala.collection.immutable.HashSet

object FilteredIndex {
  /**
   * FilteredIndex is a convenience object used to access an RDD[WarcRecord] containing results with broad filters that
   * apply to all of our intended queries.
   *
   * Intended usage is to call FilteredIndex.get(), which will return the filtered RDD[WarcRecord].
   *
   * FilteredIndex.filter() can also be used to return a DataFrame of filtered records.
   *
   * FilteredIndex.view_sample(N) prints the output of the first N records of FilteredIndex.filter() and is a convenience
   * method for debugging/improving our filters.
   *  */

  def filter(crawl: String, initial_partitions: Int=256, final_partitions: Int=0): DataFrame = {
    /** Filters the CCIndex with mutually agreed filters that should work for all our queries.
     * @param crawl - the crawl to filter the index by. Must be one of cc.idx.IndexUtil.crawls
     * @param intial_partitions - the number of partitions to sort the CCIndex by for processing
     * @param final_partitions - the number of to repartition to after processing if > 0. Else, no repartitioning is done.
     */

    val spark = AppSparkSession()

    val df = IndexUtil.load(spark).repartition(initial_partitions)

    val techJob = techJobTerms.map(_.toLowerCase)

    def filterString(line: String): Boolean = {
      line.split("(/|_|%|-|\\?|=|\\(|\\)|&|\\+|\\.)").find{
        case word => techJob.contains(word.toLowerCase)
      }.isDefined
    }

    val filterString_udf = udf(filterString _)

    val res = df
      .where(
        col("crawl") === crawl &&
        col("subset") === "warc" &&
        col("content_languages") === "eng" &&
        col("url_host_tld") === "com" &&
        col("fetch_status") === 200 &&
        col("content_mime_type") === "text/html" &&
        (col("url_path").contains("job") || col("url_path").contains("jobs") || col("url_path").contains("career") || col("url_path").contains("careers")) &&
        filterString_udf(col("url_path")) === true)

    if (final_partitions > 0)
      return res.repartition(final_partitions)
    
    return res
  }

  def get(crawl: String): RDD[WarcRecord] = {
    /** Wrapper function to get a filtered index & load the corresponding WARC records.
      *
      * @param crawl - the crawl to filter the index by. Must be one of cc.idx.IndexUtil.crawls
      */
    return WarcUtil.loadFiltered(filter(crawl))
  }

  def view_sample(crawl: String, num_samples: Int=1000): Unit = {
    /** Function to view a sample of the filtered index records.
      * 
      * @param crawl - the crawl to filter the index by. Must be one of cc.idx.IndexUtil.crawls
      */
    filter(crawl)
    .take(num_samples)
    .foreach(row => row.toSeq.foreach(println))
  }

  val techJobTerms = HashSet(
    "embedded",
    "printer",
    "imaging",
    "devops",
    "database",
    "program",
    "network",
    "accounting",
    "informatics",
    "ios",
    "prolog",
    "sql",
    "test",
    "labview",
    "aws",
    "ocaml",
    "design",
    "firewall",
    "oracle",
    "rust",
    "ict",
    "technologist",
    "server",
    "powershell",
    "css",
    "applications",
    "system",
    "scientific",
    "graphic",
    "professor",
    "clojure",
    "matlab",
    "master",
    "integratordata",
    "integration",
    "uat",
    "dba",
    "systems",
    "pytorch",
    "project",
    "unix",
    "steward",
    "gis",
    "programming",
    "domain",
    "react",
    "aspnet",
    "soa",
    "tensorflow",
    "data",
    "intelligence",
    "gcp",
    "emr",
    "it",
    "cobol",
    "networking",
    "qa",
    "voip",
    "google",
    "pascal",
    "programmer",
    "typescript",
    "associate",
    "crm",
    "risk",
    "haskell",
    "lotus",
    "wireless",
    "sqoop",
    "computer",
    "processing",
    "perl",
    "javaee",
    "vice",
    "decision",
    "firmware",
    "instructor",
    "service",
    "sparksql",
    "testing",
    "dart",
    "communications",
    "java",
    "net",
    "tester",
    "telecommunications",
    "science",
    "ssrs",
    "expert",
    "technology",
    "excel",
    "web",
    "exchange",
    "javaux",
    "hardware",
    "internet",
    "kafka",
    "logo",
    "cissp",
    "machinelearning",
    "implementation",
    "machine",
    "scrum",
    "analysttechnical",
    "hadoop",
    "peoplesoft",
    "ux",
    "cybersecurity",
    "apache",
    "representative",
    "processor",
    "vbscript",
    "deep",
    "spark",
    "julia",
    "cyber",
    "cerner",
    "technician",
    "ehr",
    "microstrategy",
    "hive",
    "php",
    "mainframe",
    "aix",
    "lua",
    "db2",
    "software",
    "scala",
    "deeplearning",
    "tech",
    "visual",
    "linux",
    "powerpoint",
    "telecom",
    "groovy",
    "mql4",
    "webmaster",
    "keras",
    "backend",
    "python",
    "hack",
    "sklearn",
    "zookeeper",
    "mysql",
    "lisp",
    "analytics",
    "c",
    "siebel",
    "delphi",
    "rails",
    "cms",
    "flume",
    "microsoft",
    "c#",
    "kotlin",
    "html",
    "ruby",
    "platform",
    "basic",
    "mule",
    "frontend",
    "cio",
    "fortran",
    "swift",
    "game",
    "apex",
    "architect",
    "arduino",
    "analyst",
    "mainframe",
    "ubuntu",
    "games",
    "digital",
    "functional",
    "auditor",
    "cryptographer",
    "windows",
    "ms",
    "bash",
    "javascript",
    "pl",
    "analytic",
    "android",
    "hdfs",
    "mlops",
    "technical",
    "sharepoint")
}
