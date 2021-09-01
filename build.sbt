name := "JobAdAnalytics"

version := "0.3"

scalaVersion := "2.11.12"

resolvers += Resolver.url("aws-repo", url("https://s3.us-east-2.amazonaws.com/us-east-2-emr-artifacts/emr-5.8.3/repos/maven/"))(Resolver.ivyStylePatterns)

libraryDependencies ++= Seq(
        "org.apache.hadoop" % "hadoop-common" % "2.10.1",
        "org.apache.hadoop" % "hadoop-client" % "2.10.1",
        "org.apache.hadoop" % "hadoop-aws" % "2.10.1",
        "org.apache.spark" %% "spark-core" % "2.2.0",
        "org.apache.spark" %% "spark-sql" % "2.2.0" 
)

trapExit := false