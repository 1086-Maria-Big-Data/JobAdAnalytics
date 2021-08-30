package cc.idx.values

class FilterOperandFunction extends FilterOperand.FilterOperand {
    type Expression = String

    def id: Int = 0

    def $lt(other: String): Expression = this.toString + " < " + other
    def $gt(other: String): Expression = this.toString + " > " + other
    def $gte(other: String): Expression = this.toString + " >= " + other
    def $lte(other: String): Expression = this.toString + " <= " + other
    def $eq(other: String): Expression = this.toString + " = " + other
    def $neq(other: String): Expression = this.toString + " <> " + other
    def in(others: Seq[String]): Expression = this.toString + " IN(" + others.mkString(", ") + ")"
    def like(other: String): Expression = this.toString + " LIKE " + other
    def rlike(other: String): Expression = this.toString + " RLIKE " + other
    // def years(other: Seq[String]): Expression = this.toString in 
}