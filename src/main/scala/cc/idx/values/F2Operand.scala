package cc.idx.values

trait F2Operand extends Enumeration { this: F1Operand =>
    type F2Operand = Value

    val Redirect    = Value("fetch_redirect")
    val Charset     = Value("content_charset")
    val Language    = Value("content_languages")
    val ContTrunc   = Value("content_truncated")

    def f2OMember(member: Value): Boolean = {
        member.isInstanceOf[F2Operand]
    }
}