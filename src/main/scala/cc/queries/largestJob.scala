package cc.queries

import cc.idx.{CCIdxMain, FilteredIndex, IndexUtil}



object largestJob extends Queries {
  def main(args: Array[String]) = {
    IndexUtil.write(FilteredIndex.filter(), "s3a://maria-1086/Devin-Testing/outputs/test-write", true, 24)
  }
  override def run(): Unit = {
  }


}