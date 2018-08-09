package com.excercise

import org.apache.log4j.Logger
import org.apache.spark.sql._
import java.io._

object SparkExercise extends Serializable {
  def main(arg: Array[String]) {

    val log = Logger.getLogger(getClass.getName)

    try {
      val spark = SparkSession
        .builder()
        .appName("Exercise1")
        .config("spark.master", "local[*]")
        .getOrCreate()
      val crimeDf = spark.read.format("csv").option("header", "true").load("./src/main/resources/exercise1/SacramentocrimeJanuary2006.csv")

      val uniqueCrime  = crimeDf.drop("district")
                      .drop("beat")
                      .drop("grid")
                      .drop("latitude")
                      .drop("longitude")
                      .distinct().cache()

      val hourseDf = spark.read.format("csv").option("header", "true").load("./src/main/resources/exercise1/Sacramentorealestatetransactions.csv")
      val uniqueHouse  = hourseDf.drop("city")
                                .drop("zip")
                                .drop("state")
                                .drop("beds")
                                .drop("baths")
                                .drop("sq__ft")
                                .drop("type")
                                .drop("sq__ft")
                                .drop("latitude")
                                .drop("longitude")
                                .filter(hourseDf("sale_date").isNotNull)
                                .filter(hourseDf("price").cast("int") > 0)
                                .withColumnRenamed("street" ,"address")
                                .drop("sale_date")
                                .drop("price")
                                .distinct().cache()


      val uniqueCrimeCollect = uniqueCrime.count()
      val uniqueHouseCollect = uniqueHouse.count()

      val crimeWithinHouse = uniqueCrime.join(uniqueHouse, "address").collect()
      val file = new File("./result1.txt")
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("Total Crime:" + uniqueCrimeCollect.toString + "\n")
      bw.write("Total House:" + uniqueHouseCollect.toString+ "\n")
      crimeWithinHouse.foreach(x => {
        bw.write("Address: " + x.getString(0) + "Date: " + x.getString(1) + "Crime: " + x.getString(2) + "Code: " + x.getString(3)+ "\n")
      })

      spark.close()
      bw.close()
    }
    catch {
      case ex: Exception =>
        log.error("General error", ex)
        throw ex
    }

  }

}