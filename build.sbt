name := "JobAdAnalytics"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
        // "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.56",
        "org.apache.hadoop" % "hadoop-common" % "2.7.3" % "provided",
        "org.apache.hadoop" % "hadoop-client" % "2.7.3" % "provided",
        "org.apache.hadoop" % "hadoop-aws" % "2.7.3" % "provided",
        "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
        "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
        "com.github.helgeho" %% "archivespark" % "3.0.1" % "provided"
)

// trapExit := false