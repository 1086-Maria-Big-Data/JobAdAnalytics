package cc.idx

import cc.warc.WarcUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
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

    return df
      .select("url_surtkey", "fetch_time", "url", "content_mime_type", "fetch_status", "content_digest", "fetch_redirect", "warc_segment", "warc_record_length", "warc_record_offset", "warc_filename")
      .where(col("crawl") === "CC-MAIN-2021-10" && col("subset") === "warc").cache
      .where(col("fetch_status") === 200).cache
      .where(col("content_languages") === "eng").cache
      .where(col("content_mime_type") === "text/html")
      .where(col("url_host_tld") === "com")
      .where(col("url_path").rlike("(?i)^(?=.*(job[s]{0,1}\\.|career[s]{0,1}\\.|/job[s]{0,1}/|/career[s]{0,1}/))(?=.*(" + techJobTerms.mkString("|") + ")).*$"))
      .repartition(final_partition)
  }

  def get(): RDD[WarcRecord] = {
    return WarcUtil.loadFiltered(filter())
  }

  def view_sample():Unit ={
    filter().take(10000).foreach(row => row.toSeq.foreach(println))
  }

  val techJobTerms = Seq("Sharepoint",
    "Application-Analyst",
    "VP-Informatics",
    "Application\\+Analyst",
    "MS-SQL",
    "Web-Developer",
    "Computer\\+Networking\\+Instructor",
    "Computer%20Scientist",
    "IT%20Quality%20Analyst",
    "Director-of-User-Experience",
    "Computer-Information-Systems",
    "Director\\+of\\+User\\+Experience",
    "Database\\+Administrator",
    "Firmware%20Manager",
    "Network%20Controller",
    "Storage-Engineer",
    "Database%20Administrator",
    "Director%20of%20User%20Experience",
    "Computer\\+Information\\+Systems",
    "Decision-Support-Manager",
    "Health-Informatics",
    "Computer%20Networking%20Instructor",
    "Web\\+Developer",
    "MS\\+SQL",
    "Application%20Analyst",
    "VP\\+Informatics",
    "Director-Oracle",
    "MS%20SQL",
    "Web%20Developer",
    "Intrusion-Analyst",
    "Health\\+Informatics",
    "Decision\\+Support\\+Manager",
    "Computer%20Information%20Systems",
    "Solution-Coordinator",
    "Automation-Application-Engineer",
    "Storage\\+Engineer",
    "Network-Security-Engineer",
    "Storage%20Engineer",
    "Automation\\+Application\\+Engineer",
    "Solution\\+Coordinator",
    "Tech-Intern",
    "Decision%20Support%20Manager",
    "Health%20Informatics",
    "Intrusion\\+Analyst",
    "Technical-Service-Engineer",
    "Programming",
    "Director\\+Oracle",
    "VP%20Informatics",
    "Director%20Oracle",
    "Data-Analyst",
    "Technical\\+Service\\+Engineer",
    "Intrusion%20Analyst",
    "EMR",
    "Computer-Hardware-Engineer",
    "Tech\\+Intern",
    "Tech%20Intern",
    "Solution%20Coordinator",
    "User-Experience-Manager",
    "Automation%20Application%20Engineer",
    "Database-Developer",
    "Database\\+Developer",
    "Database%20Developer",
    "IT-Program-Engagement-Director",
    "Network\\+Security\\+Engineer",
    "Network%20Security%20Engineer",
    "CISSP",
    "Network-Consultant",
    "IT\\+Program\\+Engagement\\+Director",
    "SQL-Developer",
    "User\\+Experience\\+Manager",
    "Computer\\+Hardware\\+Engineer",
    "Computer%20Hardware%20Engineer",
    "Implementation",
    "JavaScript-Developer",
    "Information-Technology-Professor",
    "Technical%20Service%20Engineer",
    "Data\\+Analyst",
    "Technical-Specialist",
    "Systems-Test-Analyst",
    "Technical\\+Specialist",
    "Systems\\+Test\\+Analyst",
    "Data%20Analyst",
    "Java-Developer",
    "Information\\+Technology\\+Professor",
    "JavaScript\\+Developer",
    "Storage",
    "Microstrategy-Architect",
    "User%20Experience%20Manager",
    "SQL\\+Developer",
    "IT%20Program%20Engagement%20Director",
    "Network\\+Consultant",
    "Systems-Designer",
    "SQL%20Developer",
    "Telecom-Coordinator",
    "Microstrategy\\+Architect",
    "Hardware",
    "JavaScript%20Developer",
    "Information%20Technology%20Professor",
    "Java\\+Developer",
    "DevOps",
    "Systems%20Test%20Analyst",
    "Technical%20Specialist",
    "VP-Integration",
    "Data-Manager",
    "VP\\+Integration",
    "Data\\+Manager",
    "Java%20Developer",
    "Service-Desk-Director",
    "Data%20Manager",
    "Data-Collector",
    "Cryptographer",
    "GIS-Analyst",
    "Software-Intern",
    "Microstrategy%20Architect",
    "Supervisory-IT-Specialist",
    "Telecom\\+Coordinator",
    "Supervisory\\+IT\\+Specialist",
    "Supervisory%20IT%20Specialist",
    "Object-Oriented-Programmer",
    "Object\\+Oriented\\+Programmer",
    "Object%20Oriented%20Programmer",
    "Oracle-DBA",
    "Oracle\\+DBA",
    "Systems\\+Designer",
    "Network%20Consultant",
    "Systems%20Designer",
    "Oracle%20DBA",
    "Director-Decision-Support",
    "Telecom%20Coordinator",
    "Software\\+Intern",
    "GIS\\+Analyst",
    "Technical-Training",
    "Data\\+Collector",
    "Service\\+Desk\\+Director",
    "VP%20Integration",
    "Knowledge-Manager",
    "Software-Test-Technician",
    "Service%20Desk%20Director",
    "Data%20Collector",
    "Technical\\+Training",
    "GIS%20Analyst",
    "Software%20Intern",
    "Director-Web",
    "Director\\+Decision\\+Support",
    "Windows-Software-Engineer",
    "Exchange-Administrator",
    "Information-Security-Manager",
    "Exchange\\+Administrator",
    "Windows\\+Software\\+Engineer",
    "Director%20Decision%20Support",
    "Director\\+Web",
    "IT-Project-Manager",
    "Geospatial-Analyst",
    "Technical%20Training",
    "Geospatial\\+Analyst",
    "Director-Data",
    "UX-Designer",
    "Software\\+Test\\+Technician",
    "Knowledge\\+Manager",
    "Software%20Test%20Technician",
    "UX\\+Designer",
    "Director\\+Data",
    "Geospatial%20Analyst",
    "Cyber-Security",
    "IT\\+Project\\+Manager",
    "Director%20Web",
    "Data-Migration-Lead",
    "Windows%20Software%20Engineer",
    "Exchange%20Administrator",
    "Information\\+Security\\+Manager",
    "Manager-Unix",
    "Database-Analyst",
    "Manager\\+Unix",
    "Database\\+Analyst",
    "Data\\+Migration\\+Lead",
    "IT-Auditor",
    "Cyber\\+Security",
    "IT%20Project%20Manager",
    "Web-AnalystMainframe-Developer",
    "Director%20Data",
    "UX%20Designer",
    "VP-Software-Support",
    "VP\\+Software\\+Support",
    "VP%20Software%20Support",
    "Software-Testing",
    "Knowledge%20Manager",
    "Application-Support-Analyst",
    "Software\\+Testing",
    "Tech-Support",
    "Tech\\+Support",
    "Data-Coordinator",
    "Web\\+AnalystMainframe\\+Developer",
    "Business-Analyst-",
    "Business\\+Analyst\\+",
    "Business%20Analyst%20",
    "IT-Project-Coordinator",
    "IT\\+Project\\+Coordinator",
    "Cyber%20Security",
    "IT\\+Auditor",
    "IT%20Auditor",
    "Software-Product-Manager",
    "Data%20Migration%20Lead",
    "Database%20Analyst",
    "Manager%20Unix",
    "Information%20Security%20Manager",
    "Business-Systems-Manager",
    "Client-Server-Programmer",
    "Business-Intelligence",
    "Client\\+Server\\+Programmer",
    "Software\\+Product\\+Manager",
    "Network-Security",
    "IT%20Project%20Coordinator",
    "Web%20AnalystMainframe%20Developer",
    "Data\\+Coordinator",
    "Tech%20Support",
    "Software%20Testing",
    "Application\\+Support\\+Analyst",
    "Technical-Assistant",
    "Net-Developer",
    "Technical\\+Assistant",
    "Data%20Coordinator",
    "GIS-Manager",
    "Graphic-Design-Intern",
    "Network\\+Security",
    "Software%20Product%20Manager",
    "Business\\+Intelligence",
    "Client%20Server%20Programmer",
    "Business\\+Systems\\+Manager",
    "VoIP-Engineer",
    "Business%20Systems%20Manager",
    "VoIP\\+Engineer",
    "SQL-DBA",
    "Business%20Intelligence",
    "SQL\\+DBA",
    "IT-Risk-Analyst",
    "Network%20Security",
    "Graphic\\+Design\\+Intern",
    "GIS\\+Manager",
    "Director-Information",
    "Technical%20Assistant",
    "Director\\+Information",
    "Net\\+Developer",
    "Net%20Developer",
    "Application%20Support%20Analyst",
    "Help-Desk",
    "Director%20Information",
    "Solutions-Architect",
    "GIS%20Manager",
    "Graphic%20Design%20Intern",
    "Information-Assurance",
    "IT\\+Risk\\+Analyst",
    "SQL%20DBA",
    "Software-Installer",
    "VoIP%20Engineer",
    "Technology-Adoption-Manager",
    "Information-Security-Officer",
    "Software\\+Installer",
    "Engineering-Programmer",
    "IT%20Risk%20Analyst",
    "Information\\+Assurance",
    "Director-IT-Project",
    "Platform-Engineer",
    "Solutions\\+Architect",
    "Data-Reporting-Analyst",
    "Help\\+Desk",
    "Manager-Hardware",
    "Help%20Desk",
    "Manager\\+Hardware",
    "Data\\+Reporting\\+Analyst",
    "Solutions%20Architect",
    "Platform\\+Engineer",
    "Director\\+IT\\+Project",
    "Information%20Assurance",
    "Enterprise-Data-Architect",
    "Engineering\\+Programmer",
    "Enterprise\\+Data\\+Architect",
    "Software%20Installer",
    "Information\\+Security\\+Officer",
    "Technology\\+Adoption\\+Manager",
    "Information%20Security%20Officer",
    "Big-Data",
    "Enterprise%20Data%20Architect",
    "Big\\+Data",
    "Engineering%20Programmer",
    "Technical-AnalystTechnical-Consultant",
    "Director%20IT%20Project",
    "Technical\\+AnalystTechnical\\+Consultant",
    "Platform%20Engineer",
    "Application-Coordinator",
    "Data%20Reporting%20Analyst",
    "Manager%20Hardware",
    "iOS-Developer",
    "Functional-Analyst",
    "iOS\\+Developer",
    "PHP",
    "Application\\+Coordinator",
    "GIS-Coordinator",
    "Technical%20AnalystTechnical%20Consultant",
    "Computer-Science-Intern",
    "PL-SQL-Developer",
    "Big%20Data",
    "PL\\+SQL\\+Developer",
    "IT-Compliance-Analyst",
    "Wireless-Network-Engineer",
    "IT\\+Compliance\\+Analyst",
    "Technology%20Adoption%20Manager",
    "IT%20Compliance%20Analyst",
    "Wireless\\+Network\\+Engineer",
    "PL%20SQL%20Developer",
    "Web-Operations-Specialist",
    "Computer\\+Science\\+Intern",
    "Web\\+Operations\\+Specialist",
    "Business-Continuity",
    "GIS\\+Coordinator",
    "Business\\+Continuity",
    "Application%20Coordinator",
    "Embedded-Software",
    "Functional\\+Analyst",
    "Embedded\\+Software",
    "iOS%20Developer",
    "Embedded%20Software",
    "Functional%20Analyst",
    "Data-Architect",
    "Business%20Continuity",
    "GIS%20Coordinator",
    "Web%20Operations%20Specialist",
    "Computer%20Science%20Intern",
    "Analytic-Programmer",
    "Wireless%20Network%20Engineer",
    "Platform-Architect",
    "Backup-Administrator",
    "Platform\\+Architect",
    "Backup\\+Administrator",
    "Security-Administrator",
    "Analytic\\+Programmer",
    "Security\\+Administrator",
    "IT-Manager",
    "Analytics",
    "User-Experience-Researcher",
    "Reporting-Analyst",
    "User\\+Experience\\+Researcher",
    "Data\\+Architect",
    "Sharepoint-Manager",
    "Information-Management-Specialist",
    "Clinical-Systems-Analyst",
    "Information\\+Management\\+Specialist",
    "Clinical\\+Systems\\+Analyst",
    "Sharepoint\\+Manager",
    "Data%20Architect",
    "Reporting\\+Analyst",
    "User%20Experience%20Researcher",
    "CMS-Expert",
    "IT\\+Manager",
    "Security%20Administrator",
    "Analytic%20Programmer",
    "Backup%20Administrator",
    "Platform%20Architect",
    "CIO",
    "MySQL-DBA",
    "Firewall-Engineer",
    "IT%20Manager",
    "CMS\\+Expert",
    "GIS-Administrator",
    "Reporting%20Analyst",
    "Systems-Coordinator",
    "Sharepoint%20Manager",
    "Clinical%20Systems%20Analyst",
    "Information%20Management%20Specialist",
    "Android-Developer",
    "Android\\+Developer",
    "Android%20Developer",
    "Imaging-Specialist",
    "Requirements-Analyst",
    "Systems\\+Coordinator",
    "Threat",
    "GIS\\+Administrator",
    "CMS%20Expert",
    "Web-Development-Intern",
    "Firewall\\+Engineer",
    "Firewall%20Engineer",
    "Information-Assurance-Analyst",
    "MySQL\\+DBA",
    "Storage-Consultant",
    "Security-Auditor",
    "MySQL%20DBA",
    "Security\\+Auditor",
    "Information\\+Assurance\\+Analyst",
    "Web\\+Development\\+Intern",
    "Designer-Writer",
    "GIS%20Administrator",
    "Implementation-Specialist",
    "Requirements\\+Analyst",
    "Systems%20Coordinator",
    "Imaging\\+Specialist",
    "COBOL",
    "Imaging%20Specialist",
    "Application-Manager",
    "Requirements%20Analyst",
    "Application\\+Manager",
    "Implementation\\+Specialist",
    "Backend-Developer",
    "Designer\\+Writer",
    "Web%20Development%20Intern",
    "Information%20Assurance%20Analyst",
    "Security%20Auditor",
    "ASPNet-Developer",
    "Storage\\+Consultant",
    "ASPNet\\+Developer",
    "Delivery-Architect",
    "Director-Telecommunications",
    "Technical-Manager",
    "Designer%20Writer",
    "Technical\\+Manager",
    "Technical%20Manager",
    "Backend\\+Developer",
    "Implementation%20Specialist",
    "Application%20Manager",
    "Implementation-Coordinator",
    "Implementation\\+Coordinator",
    "Implementation%20Coordinator",
    "Requirements-Manager",
    "Sharepoint-Developer",
    "Requirements\\+Manager",
    "Data-Processor",
    "Sharepoint\\+Developer",
    "Requirements%20Manager",
    "Enterprise-Architect",
    "Computer-Forensics",
    "Enterprise\\+Architect",
    "Backend%20Developer",
    "IT-Trainee",
    "Hadoop",
    "IT\\+Trainee",
    "ICT-Analyst",
    "Director\\+Telecommunications",
    "Delivery\\+Architect",
    "ASPNet%20Developer",
    "Storage%20Consultant",
    "DB2-DBA",
    "Delivery%20Architect",
    "DB2\\+DBA",
    "Director%20Telecommunications",
    "ICT\\+Analyst",
    "IT%20Trainee",
    "Systems-Integration-Engineer",
    "Enterprise%20Architect",
    "Computer\\+Forensics",
    "Financial-Systems-Analyst",
    "Sharepoint%20Developer",
    "Financial\\+Systems\\+Analyst",
    "Data\\+Processor",
    "Data%20Processor",
    "Financial%20Systems%20Analyst",
    "Epic-Analyst",
    "Computer%20Forensics",
    "Epic\\+Analyst",
    "Release-Coordinator",
    "Systems\\+Integration\\+Engineer",
    "Release\\+Coordinator",
    "Technical-Director",
    "ICT%20Analyst",
    "Information-Assurance-Engineer",
    "DB2%20DBA",
    "Information\\+Assurance\\+Engineer",
    "Application-Support-Manager",
    "Chief-Technology-Officer",
    "Application\\+Support\\+Manager",
    "Information%20Assurance%20Engineer",
    "IT-Architecture-Analyst",
    "Portal-Administrator",
    "Technical\\+Director",
    "Portal\\+Administrator",
    "Release%20Coordinator",
    "Systems%20Integration%20Engineer",
    "Epic%20Analyst",
    "Integration-Specialist",
    "Implementation-Director",
    "Integration\\+Specialist",
    "Software-Configuration",
    "Integration%20Specialist",
    "Software\\+Configuration",
    "Implementation\\+Director",
    "DevOps-Engineer",
    "GIS-Database-Administrator",
    "Systems-Architect",
    "Portal%20Administrator",
    "Systems\\+Architect",
    "Technical%20Director",
    "IT\\+Architecture\\+Analyst",
    "Network-Planner",
    "Chief\\+Technology\\+Officer",
    "Network\\+Planner",
    "Application%20Support%20Manager",
    "Network%20Planner",
    "Chief%20Technology%20Officer",
    "IT%20Architecture%20Analyst",
    "Network-Internship",
    "Systems%20Architect",
    "IT",
    "GIS\\+Database\\+Administrator",
    "Internet",
    "DevOps\\+Engineer",
    "Implementation%20Director",
    "Software%20Configuration",
    "Firmware-Engineer",
    "Data-Operations-Manager",
    "DevOps%20Engineer",
    "Engineering-Systems-Analyst",
    "Technical",
    "Engineering\\+Systems\\+Analyst",
    "GIS%20Database%20Administrator",
    "Network-Coordinator",
    "Network\\+Internship",
    "DBA-Manager",
    "DBA\\+Manager",
    "AIX-Administrator",
    "Computer-Forensics-Analyst",
    "AIX\\+Administrator",
    "IT-Compliance-Manager",
    "Computer\\+Forensics\\+Analyst",
    "AIX%20Administrator",
    "DBA%20Manager",
    "Network%20Internship",
    "Network\\+Coordinator",
    "Integration-Developer",
    "Engineering%20Systems%20Analyst",
    "Integration\\+Developer",
    "Technology-Officer",
    "Data-Center",
    "Data\\+Operations\\+Manager",
    "Data\\+Center",
    "Data%20Operations%20Manager",
    "EMR-Specialist",
    "Firmware\\+Engineer",
    "EMR\\+Specialist",
    "Lotus-Notes",
    "Data%20Center",
    "Technology\\+Officer",
    "Integration%20Developer",
    "Network%20Coordinator",
    "Director-IT",
    "Mule-Developer",
    "VP-Information-Technology",
    "Mule\\+Developer",
    "Computer%20Forensics%20Analyst",
    "VP\\+Information\\+Technology",
    "Network-Designer",
    "VP%20Information%20Technology",
    "IT\\+Compliance\\+Manager",
    "Network\\+Designer",
    "Financial-Systems-Administrator",
    "Mule%20Developer",
    "Director\\+IT",
    "Scrum-Master",
    "GIS-Scientist",
    "Technology%20Officer",
    "Lotus\\+Notes",
    "Julia",
    "EMR%20Specialist",
    "Firmware%20Engineer",
    "Solutions-Engineer",
    "JavaScript",
    "Lotus%20Notes",
    "GIS\\+Scientist",
    "Accounting-Systems-Analyst",
    "Scrum\\+Master",
    "Director%20IT",
    "Accounting\\+Systems\\+Analyst",
    "Financial\\+Systems\\+Administrator",
    "Director-Database",
    "Network%20Designer",
    "IT%20Compliance%20Manager",
    "Director-Information-Security",
    "Director\\+Database",
    "Financial%20Systems%20Administrator",
    "Accounting%20Systems%20Analyst",
    "Chief-Information-Security-Officer",
    "Help-Desk-Internship",
    "Scrum%20Master",
    "GIS%20Scientist",
    "Director-Data-Management",
    "Scratch",
    "Solutions\\+Engineer",
    "Director\\+Data\\+Management",
    "Business-Intelligence-Developer",
    "Director%20Data%20Management",
    "Solutions%20Engineer",
    "Python",
    "EHR-Trainer",
    "Applications-Scientist",
    "Chief\\+Information\\+Security\\+Officer",
    "Information-Systems-Coordinator",
    "Help\\+Desk\\+Internship",
    "Director-Business-Systems",
    "Director%20Database",
    "Director\\+Information\\+Security",
    "Director\\+Business\\+Systems",
    "Director%20Business%20Systems",
    "Domain-Architect",
    "Systems-Engineer",
    "Director%20Information%20Security",
    "Systems\\+Engineer",
    "Systems%20Engineer",
    "Manager-Net",
    "Report-Programmer",
    "Information\\+Systems\\+Coordinator",
    "Report\\+Programmer",
    "Report%20Programmer",
    "Oracle-Database-Manager",
    "Help%20Desk%20Internship",
    "Chief%20Information%20Security%20Officer",
    "Applications\\+Scientist",
    "EHR\\+Trainer",
    "COBOL",
    "Business\\+Intelligence\\+Developer",
    "Data-Assistant",
    "Business%20Intelligence%20Developer",
    "Systems-IntegratorData-Steward",
    "Data\\+Assistant",
    "Data%20Assistant",
    "Systems\\+IntegratorData\\+Steward",
    "Systems%20IntegratorData%20Steward",
    "Data-Scientist",
    "Java",
    "Data\\+Scientist",
    "EHR%20Trainer",
    "Applications%20Scientist",
    "Manager-Android",
    "Solution-Manager",
    "Oracle\\+Database\\+Manager",
    "Information%20Systems%20Coordinator",
    "Manager\\+Net",
    "Solution\\+Manager",
    "Network-Management-Specialist",
    "Domain\\+Architect",
    "Solution%20Manager",
    "Manager%20Net",
    "Software-Development-Manager",
    "Oracle%20Database%20Manager",
    "Software\\+Development\\+Manager",
    "Manager\\+Android",
    "GIS",
    "Ruby-on-Rails-Developer",
    "Data%20Scientist",
    "Fortran",
    "Network-Engineer",
    "Perl",
    "Ruby\\+on\\+Rails\\+Developer",
    "Network\\+Engineer",
    "Business-Intelligence-Analyst",
    "COBOL-Programmer",
    "Manager%20Android",
    "Business\\+Intelligence\\+Analyst",
    "Software%20Development%20Manager",
    "Reporting-Coordinator",
    "SOA-Engineer",
    "Systems-Administrator",
    "Network-Manager",
    "Network\\+Management\\+Specialist",
    "Network\\+Manager",
    "Domain%20Architect",
    "Network%20Manager",
    "Network%20Management%20Specialist",
    "Systems\\+Administrator",
    "Reporting\\+Coordinator",
    "SOA\\+Engineer",
    "Business%20Intelligence%20Analyst",
    "VP-Software-Engineering",
    "COBOL\\+Programmer",
    "Network%20Engineer",
    "Ruby%20on%20Rails%20Developer",
    "ABAP",
    "Cerner-Analyst",
    "Information-Management-Officer",
    "Cerner\\+Analyst",
    "Cerner%20Analyst",
    "Information\\+Management\\+Officer",
    "Information%20Management%20Officer",
    "Communications-Engineer",
    "VP\\+Software\\+Engineering",
    "Communications\\+Engineer",
    "VP%20Software%20Engineering",
    "Mainframe-Systems-Engineer",
    "COBOL%20Programmer",
    "Geospatial",
    "SSRS-Developer",
    "Enterprise-Engineer",
    "SSRS\\+Developer",
    "SSRS%20Developer",
    "Enterprise\\+Engineer",
    "Enterprise%20Engineer",
    "Game-Designer",
    "SOA%20Engineer",
    "Reporting%20Coordinator",
    "Oracle-Database-Analyst",
    "Solution-Director",
    "Systems%20Administrator",
    "Solution\\+Director",
    "Threat-Analyst",
    "Technology-Development-Intern",
    "Manager-Java",
    "Threat\\+Analyst",
    "Solution%20Director",
    "Technical-Services-Coordinator",
    "Systems-Integration-Manager",
    "Technical\\+Services\\+Coordinator",
    "Technical%20Services%20Coordinator",
    "Director-Software-Development",
    "Oracle\\+Database\\+Analyst",
    "Director\\+Software\\+Development",
    "Director%20Software%20Development",
    "Integration-Assistant",
    "Game\\+Designer",
    "Integration\\+Assistant",
    "Integration%20Assistant",
    "Director-Software",
    "Big-Data-Engineer",
    "Director\\+Software",
    "Director%20Software",
    "Big\\+Data\\+Engineer",
    "Big%20Data%20Engineer",
    "Incident-Response-Analyst",
    "Mainframe\\+Systems\\+Engineer",
    "Communications%20Engineer",
    "Data-Management-Engineer",
    "PHP-Developer",
    "Data\\+Management\\+Engineer",
    "Data%20Management%20Engineer",
    "Telecommunications-Specialist",
    "Scheme",
    "Ruby",
    "Swift",
    "Shell",
    "Prolog",
    "Scala",
    "PHP",
    "Telecommunications\\+Specialist",
    "C\\+\\+",
    "Objective-C",
    "VBScript",
    "Haskell",
    "SQL",
    "Delphi",
    "Arduino",
    "Hack",
    "MATLAB",
    "Pascal",
    "Rust",
    "Lua",
    "TypeScript",
    "Visual Basic",
    "Kotlin",
    "CSS",
    "Groovy",
    "Lisp",
    "Dart",
    "Assembly Language",
    "Bash",
    "PHP\\+Developer",
    "PHP%20Developer",
    "Clinical-Application-Manager",
    "Mainframe%20Systems%20Engineer",
    "VP-Software",
    "Incident\\+Response\\+Analyst",
    "VP\\+Software",
    "VP%20Software",
    "Siebel-Administrator",
    "Software-Development-Coordinator",
    "Siebel\\+Administrator",
    "Siebel%20Administrator",
    "Chief-Technologist",
    "Software\\+Development\\+Coordinator",
    "Software%20Development%20Coordinator",
    "Manager-Web",
    "Game%20Designer",
    "Scientific-Programmer",
    "Oracle%20Database%20Analyst",
    "Scientific\\+Programmer",
    "Scientific%20Programmer",
    "Game-Master",
    "Systems\\+Integration\\+Manager",
    "Systems%20Integration%20Manager",
    "Systems-Specialist",
    "Threat%20Analyst",
    "Systems\\+Specialist",
    "Systems%20Specialist",
    "Digital-Program-Manager",
    "Manager\\+Java",
    "Manager%20Java",
    "Technical-Support-Specialist",
    "Technology\\+Development\\+Intern",
    "Technical\\+Support\\+Specialist",
    "Technical%20Support%20Specialist",
    "Enterprise-Systems-Manager",
    "Digital\\+Program\\+Manager",
    "Enterprise\\+Systems\\+Manager",
    "Networking-Specialist",
    "Game\\+Master",
    "Software",
    "Manager\\+Web",
    "Manager%20Web",
    "Chief\\+Technologist",
    "Incident%20Response%20Analyst",
    "Business-Intelligence-Manager",
    "Clinical\\+Application\\+Manager",
    "PowerShell",
    "Telecommunications%20Specialist",
    "Data-Processing",
    "Clojure",
    "MQL4",
    "Apex",
    "LabVIEW",
    "Logo",
    "Clinical%20Application%20Manager",
    "Integration-Engineer",
    "Business\\+Intelligence\\+Manager",
    "Business%20Intelligence%20Manager",
    "Incident-Manager",
    "Chief%20Technologist",
    "Linux",
    "Unix",
    "Vice-President-Network",
    "Telecom-Assistant",
    "Oracle",
    "Telecom\\+Assistant",
    "Telecom%20Assistant",
    "Director-Systems",
    "Networking\\+Specialist",
    "Game%20Master",
    "Lotus-Notes-Developer",
    "Enterprise%20Systems%20Manager",
    "Service-Desk-Associate",
    "Lotus\\+Notes\\+Developer",
    "Lotus%20Notes%20Developer",
    "Service\\+Desk\\+Associate",
    "Service%20Desk%20Associate",
    "Portal-Architect",
    "Digital%20Program%20Manager",
    "Webmaster",
    "Technology%20Development%20Intern",
    "Integration-Director",
    "Technical-Program-Manager",
    "Portal\\+Architect",
    "Technical\\+Program\\+Manager",
    "Technical%20Program%20Manager",
    "Technology-Risk-Intern",
    "Game-Tester",
    "Networking%20Specialist",
    "Security-Researcher",
    "Director\\+Systems",
    "Director%20Systems",
    "Net-Coordinator",
    "Net\\+Coordinator",
    "Net%20Coordinator",
    "JavaUX",
    "Director-Oracle-Database",
    "Director\\+Oracle\\+Database",
    "Director%20Oracle%20Database",
    "Software-Engineering",
    "Software\\+Engineering",
    "Software%20Engineering",
    "Database",
    "Application",
    "SQL",
    "Rails",
    "Reporting",
    "Data-Warehouse",
    "Vice\\+President\\+Network",
    "Vice%20President%20Network",
    "Software-Test-Engineer",
    "Software\\+Test\\+Engineer",
    "Software%20Test%20Engineer",
    "Chief-Technical-Officer",
    "Chief\\+Technical\\+Officer",
    "Chief%20Technical%20Officer",
    "Software-QA-Manager",
    "Software\\+QA\\+Manager",
    "Software%20QA%20Manager",
    "Chief-Data-Officer",
    "Chief\\+Data\\+Officer",
    "Chief%20Data%20Officer",
    "Functional-Tester",
    "Functional\\+Tester",
    "Functional%20Tester",
    "VP-DataChief-Analytics-Officer",
    "VP\\+DataChief\\+Analytics\\+Officer",
    "VP%20DataChief%20Analytics%20Officer",
    "UAT-Tester",
    "UAT\\+Tester",
    "UAT%20Tester",
    "Chief-Informatics-Officer",
    "Chief\\+Informatics\\+Officer",
    "Chief%20Informatics%20Officer",
    "Director-Software-Quality-Assurance",
    "Director\\+Software\\+Quality\\+Assurance",
    "Director%20Software%20Quality%20Assurance",
    "Incident\\+Manager",
    "Incident%20Manager",
    "Hadoop-Developer",
    "Integration\\+Engineer",
    "Integration%20Engineer",
    "Epic-Manager",
    "Epic\\+Manager",
    "Epic%20Manager",
    "Release-Engineer",
    "Release\\+Engineer",
    "Release%20Engineer",
    "Director-Clinical-Applications",
    "Director\\+Clinical\\+Applications",
    "Director%20Clinical%20Applications",
    "Embedded-Software-Engineer",
    "Embedded\\+Software\\+Engineer",
    "Embedded%20Software%20Engineer",
    "Health-Systems-Analyst",
    "Health\\+Systems\\+Analyst",
    "Health%20Systems%20Analyst",
    "Oracle-Developer",
    "Oracle\\+Developer",
    "Oracle%20Developer",
    "Epic-Director",
    "Epic\\+Director",
    "Epic%20Director",
    "Epic",
    "Cerner",
    "Computer-Scientist",
    "Computer\\+Scientist",
    "Data\\+Processing",
    "Data%20Processing",
    "Network-Systems-Operator",
    "Network\\+Systems\\+Operator",
    "Network%20Systems%20Operator",
    "Information-Security-Analyst",
    "Information\\+Security\\+Analyst",
    "Information%20Security%20Analyst",
    "Security-Engineer",
    "Security\\+Engineer",
    "Security%20Engineer",
    "Manager-Telecom",
    "Manager\\+Telecom",
    "Manager%20Telecom",
    "Penetration-Tester",
    "Penetration\\+Tester",
    "Penetration%20Tester",
    "Telecommunications-Analyst",
    "Telecommunications\\+Analyst",
    "Telecommunications%20Analyst",
    "Security-Architect",
    "Security\\+Architect",
    "Security%20Architect",
    "Network-Controller",
    "Network\\+Controller",
    "Hadoop\\+Developer",
    "Hadoop%20Developer",
    "CRM-Consultant",
    "CRM\\+Consultant",
    "CRM%20Consultant",
    "Decision-Support-Analyst",
    "Decision\\+Support\\+Analyst",
    "Decision%20Support%20Analyst",
    "Exchange-Consultant",
    "Exchange\\+Consultant",
    "Exchange%20Consultant",
    "Director-Business-Intelligence",
    "Director\\+Business\\+Intelligence",
    "Director%20Business%20Intelligence",
    "Printer-Technician",
    "Printer\\+Technician",
    "Printer%20Technician",
    "Master-Data-Analyst",
    "Master\\+Data\\+Analyst",
    "Master%20Data%20Analyst",
    "IT-Quality-Analyst",
    "IT\\+Quality\\+Analyst",
    "Data\\+Warehouse",
    "Data%20Warehouse",
    "Peoplesoft",
    "JavaScript",
    "Oracle-Database",
    "Security\\+Researcher",
    "Oracle\\+Database",
    "Oracle%20Database",
    "Game\\+Tester",
    "Game%20Tester",
    "Software-Technician",
    "Software\\+Technician",
    "Software%20Technician",
    "Director-Game",
    "Director\\+Game",
    "Director%20Game",
    "Security-Developer",
    "Technology\\+Risk\\+Intern",
    "Technology%20Risk%20Intern",
    "Implementation-Manager",
    "Portal%20Architect",
    "Implementation\\+Manager",
    "Implementation%20Manager",
    "Business-Analyst",
    "Business\\+Analyst",
    "Business%20Analyst",
    "Release-Manager",
    "Release\\+Manager",
    "Release%20Manager",
    "Network-Director",
    "Network\\+Director",
    "Network%20Director",
    "Business-Systems-Analyst",
    "Business\\+Systems\\+Analyst",
    "Business%20Systems%20Analyst",
    "Integration-Manager",
    "Integration\\+Manager",
    "Integration%20Manager",
    "Solution-Specialist",
    "Solution\\+Specialist",
    "Solution%20Specialist",
    "Data-Center-Manager",
    "Data\\+Center\\+Manager",
    "Data%20Center%20Manager",
    "Integration\\+Director",
    "Integration%20Director",
    "Linux-System-Administrator",
    "Linux\\+System\\+Administrator",
    "Linux%20System%20Administrator",
    "Director-Hardware",
    "Computer-Technician",
    "Security\\+Developer",
    "Security%20Developer",
    "Manager-Video-Games",
    "Security%20Researcher",
    "Network-Developer",
    "Network\\+Developer",
    "Network%20Developer",
    "Business-Continuity-Manager",
    "Business\\+Continuity\\+Manager",
    "Business%20Continuity%20Manager",
    "Network",
    "Manager\\+Video\\+Games",
    "Manager%20Video%20Games",
    "Backend-Java-Developer",
    "Computer\\+Technician",
    "Director\\+Hardware",
    "Computer%20Technician",
    "Program-Architect",
    "Backend\\+Java\\+Developer",
    "Backend%20Java%20Developer",
    "Game",
    "Release-Specialist",
    "Release\\+Specialist",
    "Release%20Specialist",
    "Database-Administrator",
    "Security-Incident-Response-Engineer",
    "Program\\+Architect",
    "Security\\+Incident\\+Response\\+Engineer",
    "Director%20Hardware",
    "Systems-Manager",
    "Systems\\+Manager",
    "Systems%20Manager",
    "Manager-Linux",
    "Manager\\+Linux",
    "Manager%20Linux",
    "Windows-Administrator",
    "Windows\\+Administrator",
    "Windows%20Administrator",
    "Manager-PHP",
    "Manager\\+PHP",
    "Manager%20PHP",
    "Unix-Administrator",
    "Unix\\+Administrator",
    "Unix%20Administrator",
    "Firmware-Manager",
    "Firmware\\+Manager",
    "Security%20Incident%20Response%20Engineer",
    "Program%20Architect",
    "Technical-Support-Manager",
    "Technical\\+Support\\+Manager",
    "Technical%20Support%20Manager",
    "Director-Enterprise-Systems",
    "Director\\+Enterprise\\+Systems",
    "Director%20Enterprise%20Systems",
    "Head-Of-Digital",
    "Head\\+Of\\+Digital",
    "Head%20Of%20Digital",
    "Technical-Service-Representative",
    "Technical\\+Service\\+Representative",
    "Technical%20Service%20Representative",
    "Manager-Web-Application",
    "Manager\\+Web\\+Application",
    "Manager%20Web%20Application",
    "Service-Desk-Manager",
    "Telecommunications",
    "Service\\+Desk\\+Manager",
    "Service%20Desk%20Manager",
    "Software-Engineer",
    "Malware-Analyst",
    "Software\\+Engineer",
    "Software%20Engineer",
    "Information-Systems-Technician",
    "Malware\\+Analyst",
    "Malware%20Analyst",
    "Networking",
    "Information\\+Systems\\+Technician",
    "Information%20Systems%20Technician",
    "Information-Assurance-Officer",
    "Information\\+Assurance\\+Officer",
    "Information%20Assurance%20Officer",
    "Technical-Trainer",
    "Technical\\+Trainer",
    "Technical%20Trainer",
    "Information-Assurance-Manager",
    "Information\\+Assurance\\+Manager",
    "Information%20Assurance%20Manager",
    "Technical-Training-Manager",
    "Technical\\+Training\\+Manager",
    "Technical%20Training%20Manager",
    "Business-Continuity-Analyst",
    "Business\\+Continuity\\+Analyst",
    "Business%20Continuity%20Analyst",
    "Technical-Training-Coordinator",
    "Technical\\+Training\\+Coordinator",
    "Technical%20Training%20Coordinator",
    "Computer-Networking-Instructor")
}
