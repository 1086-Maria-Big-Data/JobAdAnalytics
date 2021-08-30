package cc.idx.values

trait F1Operand extends Enumeration { this: F2Operand =>
    type F1Operand = Value

    val URLSurtKey      = Value("url_surtkey")
    val URL             = Value("url")
    val URLHostName     = Value("url_host_name")
    val URLHostTLD      = Value("url_host_tld")
    val URLHost2nd      = Value("url_host_2nd_last_part")
    val URLHost3rd      = Value("url_host_3rd_last_part")
    val URLHost4th      = Value("url_host_4th_last_part")
    val URLHost5th      = Value("url_host_5th_last_part")
    val URLHostRegSuff  = Value("url_host_registry_suffix")
    val URLHostRegDom   = Value("url_host_registered_domain")
    val URLHostPrivSuff = Value("url_host_private_suffix")
    val URLHostPrivDom  = Value("url_host_private_domain")
    val URLProto        = Value("url_protocol")
    val URLPort         = Value("url_port")
    val URLPath         = Value("url_path")
    val URLQuery        = Value("url_query")
    val FetchTime       = Value("fetch_time")
    val FetchStatus     = Value("fetch_status")
    val ContDigest      = Value("content_digest")
    val ContMime        = Value("content_mime_type")
    val ContMimeDet     = Value("content_mime_detected")
    val WarcOffset      = Value("warc_record_offset")
    val WarcLength      = Value("warc_record_length")
    val WarcSeg         = Value("warc_segment")
    val Crawl           = Value("crawl")
    val Subset          = Value("subset")
}