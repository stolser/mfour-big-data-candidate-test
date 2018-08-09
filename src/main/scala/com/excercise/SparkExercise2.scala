package com.excercise

import org.apache.log4j.Logger
import org.apache.spark.sql._
import java.io._
import org.apache.spark.sql.functions.{to_date, from_unixtime, unix_timestamp}

import org.apache.spark.sql.SparkSession

object SparkExercise2 extends Serializable {
  def main(arg: Array[String]) {

    val log = Logger.getLogger(getClass.getName)

    try {
      val spark = SparkSession
        .builder()
        .appName("Exercise2")
        .config("spark.master", "local[*]")
        .getOrCreate()

      var temp: Dataset[Row] = null

      getListOfFiles("./src/main/resources/exercise2").foreach(file => {
        val crimeDf = spark.read.format("csv").option("header", "true").load(file.getPath)
        if(temp == null) {
          temp = crimeDf
        } else {
          temp = temp.union(crimeDf)
        }
      })

      val totalRow = temp.cache()
      val totalCrime = totalRow.drop("District")
        .drop("Neighborhood")
        .drop("OccurenceLocation")
        .drop("OccurenceCity")
        .drop("X_Coord")
        .drop("Y_Coord")
        .drop("Injury")
        .distinct().cache()

      val total = totalRow.collect().length
      totalRow.unpersist()

      val zipcodeMostCrime = totalCrime.groupBy("OccurenceZipCode").max().first() //95834->95662->95670
      //val totalCrimexx = totalCrime.filter(totalCrime("OccurenceZipCode").cast("int") === 95834).collect() //prove
      val zipcodeMostCrimeCommit = totalCrime.filter(totalCrime("ReportDate").isNotNull && totalCrime("PrimaryViolation").isNotNull).groupBy("OccurenceZipCode").max().first()

      val crimeWithDateCast =  totalCrime.withColumn("OccurenceStartDate", to_date(totalCrime("OccurenceStartDate"), "MM/dd/yyyy").cast("date"))
                                          .withColumn("OccurenceEndDate", to_date(totalCrime("OccurenceEndDate"), "MM/dd/yyyy").cast("date")).cache()


      totalCrime.unpersist()
      val crimeWithinTheYear =  crimeWithDateCast.filter(_.getDate(1).getYear  == 108)
                                                  .filter(_.getDate(2).getYear  == 108).count()

      val crimeWithMonthCast = crimeWithDateCast.withColumn("Month", from_unixtime(unix_timestamp(crimeWithDateCast("OccurenceStartDate"), "dd/MM/yy hh:mm"), "MM")).cache()
      crimeWithDateCast.unpersist()

      val monthOfMaxCrime = crimeWithMonthCast.groupBy("Month").max().first()

      spark.close()

      val file = new File("./result2.txt")
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("MostCrime ZipCode: " + zipcodeMostCrime.toString + "\n")
      bw.write("MostCrime commit ZipCode: " + zipcodeMostCrimeCommit.toString+ "\n")
      bw.write("Crimes happen this year: " + crimeWithinTheYear.toString+ "\n")
      bw.write("Month Of Crime: " + monthOfMaxCrime.toString+ "\n")
      bw.close()
    }
    catch {
      case ex: Exception =>
        log.error("General error", ex)
        throw ex
    }

  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}