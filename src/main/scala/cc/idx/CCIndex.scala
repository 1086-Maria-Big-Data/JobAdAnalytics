package cc.idx

import cc.idx.values.FilterOperand.FilterOperand
import cc.idx.values.{FilterOperand => FO}

import scala.reflect.ClassTag
import org.apache.spark.sql.{DataFrame, SparkSession}

protected class CCIndex {
    private val opds = new Array[Seq[FilterOperand]](2)

    def :+(operand: FilterOperand*): CCIndex = {
        opds(0) = operand.filter(!FO.f2OMember(_))
        opds(1) = operand.filter(FO.f2OMember(_))
        this
    }

    def :+[_: ClassTag](operands: Seq[FilterOperand]): CCIndex = this :+ (operands:_*)

    def dataFrame(spark: SparkSession): DataFrame = {
        val select = "SELECT "
        val from = " FROM "
        val where = " WHERE "
        
        val view = "temp"
        val filters = opds(0).map(fOp => fOp.toString).mkString

        val selection =(
                "url_surtkey, "
            .+( "fetch_time, "          
            .+( "url, "                 
            .+( "content_mime_type, "   
            .+( "fetch_status, "        
            .+( "content_digest, "      
            .+( "fetch_redirect, "      
            .+( "warc_record_length, "  
            .+( "warc_record_offset, "  
            .+( "warc_filename" 
        ))))))))))        
        
        IndexUtil.load(spark).createOrReplaceTempView(view)
        spark.sql(select + selection + from + view + where + filters)
    }
}

object CCIndex {
    
}
