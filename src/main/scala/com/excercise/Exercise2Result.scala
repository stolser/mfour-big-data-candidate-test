package com.excercise

case class Exercise2Result(ziodeCodeMostCrime: Seq[(String, Long)], ziodeCodeMostCrimeCommit :Seq[(String, Long)], crimeOverTheYear: Long, monthOfMaxCrime : String) extends Serializable {

}
