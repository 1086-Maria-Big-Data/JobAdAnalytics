package cc.queries

import cc.idx.FilteredIndex.techJobTerms
import cc.idx.{CCIdxMain, FilteredIndex, IndexUtil}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import spark.session.AppSparkSession

object largestJob extends Queries {
  def main(args: Array[String]) = {
    IndexUtil.write(FilteredIndex.filter("CC-MAIN-2021-10"), "s3a://maria-1086/FilteredIndex/CC-MAIN-2021-10", " ", true, 24)
  }
  override def run(): Unit = {

  }

}