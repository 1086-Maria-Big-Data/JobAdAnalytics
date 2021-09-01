name := "JobAdAnalytics"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
        "org.apache.hadoop" % "hadoop-common" % "2.7.3",
        "org.apache.hadoop" % "hadoop-client" % "2.7.3",
        "org.apache.hadoop" % "hadoop-aws" % "2.7.3",
        "org.apache.spark" %% "spark-core" % "2.2.0",
        "org.apache.spark" %% "spark-sql" % "2.2.0",
        // "org.slf4j" % "slf4j-simple" % "1.6.2" % Test
)

trapExit := false