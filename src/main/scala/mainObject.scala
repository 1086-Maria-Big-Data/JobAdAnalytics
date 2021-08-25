import cc.idx.CCIdxMain

object mainObject {
  def main(args: Array[String]): Unit = {
    println("test")

    // Sample run -- still WIP
    /**
     * Just a reminder, if you'd like to test it, you'll need to set the
     * AWS_ACCESS_KEY_ID and AWS_SECRET_ACESS_KEY environment variables
     *
     * How to get access keys:
     * AWS -> your account dropdown -> My security credentials -> Access Keys -> create new access key
     *
     * To set environment vairable in IntelliJ click on the dropdown with mainObject and edit configuration
     * then set environment variable to AWS_ACCESS_KEY_ID=******;AWS_SECRET_ACCESS_KEY=*******
     */
    CCIdxMain.main(args)
  }
}
