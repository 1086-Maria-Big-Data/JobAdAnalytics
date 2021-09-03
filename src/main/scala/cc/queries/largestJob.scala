package cc.queries

import cc.idx.IndexUtil
import cc.warc.{SuperWarc, WarcUtil}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, desc, lower, length}
import org.archive.archivespark.functions.{Html, HtmlText}
import spark.session.AppSparkSession
import sun.security.ssl.SSLContextImpl.DefaultSSLContext

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object largestJob extends Queries {
  def main(args: Array[String]): Unit = {
    val spark = AppSparkSession()

    //wordcount method

    //Step 1:

    // load job CSV
    val csv = spark.read
      .format("csv")
      .option("header", true)
      .load("s3a://maria-1086/FilteredIndex/*")

    val rdd = WarcUtil.loadFiltered(csv)
      val wordCount = spark.createDataFrame(
        rdd
          .take(5)
          .map(warc => SuperWarc(warc))
          .flatMap(warc => warc.payload(true).split(" "))
          .map(word => (word, 1))
          .groupBy(_._1)
          .map(pair => (pair._1, pair._2.map(_._2).reduce(_ + _)))
          .toSeq
      )

    //Step 2: filter for company names
    // 1. ascii only
    // 2. number and special characters
    // 3. common english articles
    // 4. common words that is not a company name
    // the goal is top50 has only company names and top100 has mostly only company names

    wordCount
      .withColumnRenamed("_1", "word")
      .withColumnRenamed("_2", "count")
      //.withColumn("word", lower(col("word")))

      //ascii encode+decode filter
      //.withColumn("word", decode(encode(col("word") ,"ascii"),"ascii"))
      //.filter(!col("word").contains("?"))

      //numbers and special characters
      .filter(!col("word").rlike("[0-9]"))
      .filter(!col("word").rlike("-|[+></,.|’#'()&!*:@]"))

      //filter lowercase
      .filter(!(col("word")===lower(col("word"))))

      //filter 2letters or below
      .filter(!(length(col("word")) <= 2))

      //common words
      .filter(!col("word").rlike("(?i)and|the|in|of|to|a|for|as|you|with|for|is|our|your|be|an|us|will|or|have|on|are|free"))
      .filter(!col("word").rlike("(?i)jobs|job|posted|experience|developer|by|get|it|we|help|but|from|home|site|resume|never|out"))
      .filter(!col("word").rlike("(?i)test|log|hr|skills|scripts|per|while|such|which|ensure|degree|not|up|science|over|both|every"))
      .filter(!col("word").rlike("(?i)I|reply|tips|first|some|my|if|new|how|comments|so|post|posts|follow|know|he|send|when|enter|recent"))
      .filter(!col("word").rlike("(?i)most|mode|should|good|even|next|complete|project|school|no|let|old|guy|why|full|occurrence|nosql"))

      .orderBy(desc("count"))
      //.show(100)
      .repartition(1)
      .write.format("csv").save("/output/output-"+ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")))


    //Step 3: mapping common words to url
    // may have url linked to multiple words
    // need to wait until the data output to see how to filter

    //Step 4: Query output

    //IndexUtil.write(wordCount_df, "s3://maria-1086/Hong-test/CCIdxMain/-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")), include_header=true, num_files=1)

    spark.stop
    System.exit(0)
  }
}