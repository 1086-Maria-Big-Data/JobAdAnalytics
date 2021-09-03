package cc.idx

import cc.warc.WarcUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.archive.archivespark.specific.warc.WarcRecord
import spark.session.AppSparkSession

object FilteredIndex {
  /**
   * FilteredIndex is a convenience object used to access an RDD[WarcRecord] containing results with broad filters that
   * apply to all of our intended queries.
   *
   * Intended usage is to call FilteredIndex.get(), which will return the filtered RDD[WarcRecord].
   *
   * FilteredIndex.filter() can also be used to return a DataFrame of filtered records.
   *
   * FilteredIndex.view_sample() prints the output of the first 100 records of FilteredIndex.filter() and is a convenience
   * method for debugging/improving our filters.
   *  */

  def filter(initial_partition: Int=256, final_partition: Int=16): DataFrame = {
    /** Filters the CCIndex with mutually agreed filters that should work for all our queries.
     *
     * @param intial_partition - the number of partitions to sort the CCIndex by for processing
     * @param final_partition - the number of partitions to pass into WarcUtil.loadfiltered
     */
    val spark = AppSparkSession()

    val df = IndexUtil.load(spark).repartition(initial_partition)

    val techJob = techJobTerms.map(_.toLowerCase)

    def filterString(line: String): Boolean = {
      line.split("(/|,|%|-)").collectFirst{
        case word => techJob.contains(word.toLowerCase)
      }.getOrElse(false)
    }

    val filterString_udf = udf(filterString _)


    return df
      .where(col("fetch_status") === 200 &&
        col("content_mime_type") === "text/html" &&
        col("content_languages") === "eng" &&
        col("subset") === "warc" &&
        col("crawl") === "CC-MAIN-2021-10" &&
        col("url_host_tld") === "com" &&
        col("url_path").rlike("(?i)^(?=.*(job[s]{0,1}\\.|career[s]{0,1}\\.|/job[s]{0,1}/|/career[s]{0,1}/))") &&
        filterString_udf(col("url_path")) === true)
      //.repartition(final_partition)
  }

  def get(): RDD[WarcRecord] = {
    return WarcUtil.loadFiltered(filter())
  }

  def view_sample():Unit ={
    filter().take(10000).foreach(row => row.toSeq.foreach(println))
  }

  val techJobTerms = Seq("embedded",
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
