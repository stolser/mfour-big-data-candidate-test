package com.excercise

class Exercise1Result (address: String) extends Serializable {

}


case class Detail(cdatetime: String, crimedescr: String, ucr_ncic_code: String)  extends Serializable {}
case class Exercise1ResultItem(address: String, detailliists: Array[Detail])  extends Serializable {}