name := "JobAdAnalytics"

version := "0.1"

scalaVersion := "2.11.12"

// resolvers += Resolver.url("emr-5.8.3-artifacts", url("https://s3.us-east-2.amazonaws.com/us-east-2-emr-artifacts/emr-8.3.0/repos/maven/"))
resolvers += Resolver.url("aws-repo", "https://s3.us-east-2.amazonaws.com/us-east-2-emr-artifacts/emr-8.3.0/repos/maven/")(Resolver.ivyStylePatterns)

libraryDependencies ++= Seq(
        // "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.56",
        // "org.apache.hadoop" % "hadoop-common" % "2.10.1",
        // "org.apache.hadoop" % "hadoop-client" % "2.10.1",
        // "org.apache.hadoop" % "hadoop-aws" % "2.10.1",
        // "org.apache.spark" %% "spark-core" % "2.4.7",
        // "org.apache.spark" %% "spark-sql" % "2.4.7"

        "org.apache.hadoop" % "hadoop-common" % "2.7.3",
        "org.apache.hadoop" % "hadoop-client" % "2.7.3-amzn-3",
        "org.apache.hadoop" % "hadoop-aws" % "2.7.3",
        "org.apache.spark" %% "spark-core" % "2.2.0",
        "org.apache.spark" %% "spark-sql" % "2.2.0"
)

// trapExit := false