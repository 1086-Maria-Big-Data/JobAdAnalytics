package cc.queries

import cc.warc

import spark.session.AppSparkSession
import cc.queries.entryLevel.spark
import org.archive.archivespark._

object entryLevel extends Queries {

  //updating this without newer version from GitHub that does not run fully yet so please excuse a few things
  //not sure if we need this
  val spark = AppSparkSession()

  //a very fast way to filter out all entry level requiring not requiring experience

  //example file in full example we bring in all paths after index specifies them
  val rdd = WarcUtil.load("s3a://commoncrawl/crawl-data/CC-MAIN-2016-36/segments/1471982290442.1/warc/CC-MAIN-20160823195810-00000-ip-10-153-172-175.ec2.internal.warc.gz")

  //we also only use first 1000 for testing
  val x=rdd.toJsonStrings.take(1000).filter(f=> f.contains("body"))
  //x.foreach(println)
  val y=x.filter(f=> f.contains("x"))
  //val y=x.filter(f=> f.contains("entry level") || f.contains("entry-level"))
  val z=y.filter(f=> f.contains("z"))
  //val z=y.filter(f=> f.contains("experience"))
  val q=z.filter(f=> !f.contains("yy"))
  //val q=z.filter(f=> !(f.contains("no experience")))

  //so we see in the example the z.length gives count of all where body contains "x" and "z"
  //we see then that q.length gives count of all where body contains "x" and "z" and not "yy"
  //for the real example we want "entry level" or "entry-level" and not ("experience" but "no experience" is ok)
  println(z.length)
  println(q.length)

  //not sure if we need this
  spark.stop
  System.exit(0)
}