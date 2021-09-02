package cc.queries

import cc.idx.FilteredIndex.techJobTerms
import cc.idx.{CCIdxMain, FilteredIndex, IndexUtil}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import spark.session.AppSparkSession



object largestJob extends Queries {
  def main(args: Array[String]) = {
    run()
  }
  override def run(): Unit = {
    val spark = AppSparkSession()

    val df = IndexUtil.load(spark)//.repartition(initial_partition)

    val techJob = techJobTerms.map(_.toLowerCase)

    def filterString(line: String): Boolean = {
      line.split("/").collectFirst{
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
        col("url_path").rlike("(?i).*(?=" + testterms + ").*$"))
      .take(100)
      .foreach(row => row.toSeq.foreach(println))



  }
  val testterms = "Sharepoint|Developer|User-Experience|Oracle|Network-Security|Programming|CISSP" +
  "|Network-Consultant|SQL|JavaScript|Information-Technology|Test-Analyst|Java|Systems-Designer||DevOps" //+
//    "|Data|Cryptographer|GIS|IT|Software|Exchange-Administrator|Information-Security|Geospatial-Analyst|UX|" +
//    "Unix|Tech-Support|Business-Systems|Client-Server|Business-Intelligence| Technical|VoIP|" +
//    "IT-Risk-Analyst|Director-Information|Help-Desk|Solutions-Architect|Information-Assurance|Technology-Adoption|" +
//    "Programmer|IT-Project|Platform-Engineer|Hardware|Enterprise|Big-Data|Application|Functional-Analyst|PHP|" +
//    "GIS-Coordinator|Computer-Science|Wireless-Network|Web-Operations|Business-Continuity|Data-Architect|" +
//    "Platform-Architect|Backup-Administrator|Security-Administrator|Analytics|Reporting-Analyst|Information-Management|" +
//    "Clinical-Systems-Analyst|CMS|CIO|Firewall|Systems-Coordinator|Imaging-Specialist|Requirements-Analyst|Threat|" +
//    "Web-Development|Storage-Consultant|Implementation|COBOL|Delivery-Architect|Telecommunications|" +
//    "Architect|Computer|Hadoop|ICT-Analyst|DB2|DBA|Systems-Integration|Systems-Analyst|Epic|Release-Coordinator|" +
//    "Chief-Technology-Officer|Portal-Administrator|Systems-Architect|Network|Internet|Firmware|AIX|Technology-Officer|" +
//    "EMR|Lotus-Notes|Systems-Administrator|Scrum-Master|Julia|Solutions-Engineer|Scratch|Python|EHR|" +
//    "Information-Systems|Domain-Architect|Systems-Engineer|Manager-Net|Android|Solution-Manager|Ruby-on-Rails|" +
//    "Fortran|Perl|Reporting-Coordinator|SOA|ABAP|Cerner|Information-Management|Communications-Engineer|Mainframe|" +
//    "Geospatial|SSRS|Game-Designer|Solution-Director|Threat-Analyst|Technology-Development|Integration-Assistant|" +
//    "Big-Data|Incident-Response-Analyst|Data-Management-Engineer|PHP|Scheme|Ruby|Swift|Shell|Prolog|Scala|PHP|" +
//    "Objective-C|VBScript|Haskell|Delphi|Arduino|Hack|MATLAB|Pascal|Rust|Lua|TypeScript|Visual-Basic|Kotlin|CSS|Groovy" +
//    "|Lisp|Dart|Assembly-Language|Bash|Siebel|Chief-Technologist|Manager-Web|Game-Master|Systems-Specialist|" +
//    "Digital-Program-Manager|PowerShell|Clojure|MQL4|Apex|LabVIEW|Logo|Incident-Manager|Linux|Unix|Telecom-Assistant|" +
//    "Director-Systems|Portal-Architect|Webmaster|Integration-Director|Net-Coordinator|JavaUX|Database|Rails|Reporting|" +
//    "Data-Warehouse|Functional-Tester|VP-DataChief-Analytics-Officer|UAT-Tester|Chief-Informatics-Officer|" +
//    "Release-Engineer|Health-Systems-Analyst|Epic|Manager-Telecom|Penetration-Tester|CRM|Decision-Support-Analyst|" +
//    "Exchange-Consultant|Printer-Technician|Peoplesoft|Release-Manager|Solution-Specialist|Business-Continuity-Manager|" +
//    "Backend|Program-Architect|Release-Specialist|Database|Systems-Manager|Windows|Digital|Service-Desk|Malware"

}