package com.excercise

import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.rdd._
import java.io._
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{collect_list, count, from_unixtime, max, min, struct, to_date, unix_timestamp}
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

      val count_exprs = totalCrime.columns.map(count(_))
      //1 Which zip codes had the most crime
      val crimeGroupByZipCodeWithMaxCount = totalCrime.groupBy("OccurenceZipCode").agg(count_exprs.head as "maxcount").cache() //.agg(max("maxcount") as "max").collect()
      val maxCount = crimeGroupByZipCodeWithMaxCount.agg(max("maxcount")).first()
      val zipCodeWithMaxCount = crimeGroupByZipCodeWithMaxCount.filter(_.getLong(1) == maxCount.getLong(0)).collect().map(row => (row.getString(0), row.getLong(1)))
        crimeGroupByZipCodeWithMaxCount.unpersist()

      //2 In those zip codes, which was the most committed crime
      val zipcodeMostCrimeCommit = totalCrime.filter(totalCrime("ReportDate").isNotNull && totalCrime("PrimaryViolation").isNotNull)
                                                .groupBy("OccurenceZipCode")
                                                .agg(count_exprs.head as "maxcount").cache()
      val maxCountCommit = zipcodeMostCrimeCommit.agg(max("maxcount")).first()
      val zipCodeWithMaxCountCommit = zipcodeMostCrimeCommit.filter(_.getLong(1) == maxCount.getLong(0)).collect().map(row => (row.getString(0), row.getLong(1)))
      zipcodeMostCrimeCommit.unpersist()


      val crimeWithDateCast =  totalCrime.withColumn("OccurenceStartDate", to_date(totalCrime("OccurenceStartDate"), "MM/dd/yyyy").cast("date"))
                                          .withColumn("OccurenceEndDate", to_date(totalCrime("OccurenceEndDate"), "MM/dd/yyyy").cast("date")).cache()


      totalCrime.unpersist()

      //3 How many times did the crimes happen over the year
      val crimeOverTheYear =  crimeWithDateCast.filter(_.getDate(1).getYear  == 108)
                                                  .filter(_.getDate(2).getYear  == 108).distinct().count()

      //4 What months had the most crime
      val crimeWithMonthCast = crimeWithDateCast.withColumn("Month", from_unixtime(unix_timestamp(crimeWithDateCast("OccurenceStartDate"), "dd/MM/yy hh:mm"), "MM")).cache()
      crimeWithDateCast.unpersist()

      val crimeGroupByMonth = crimeWithMonthCast.groupBy("Month").agg(crimeWithMonthCast.columns.map(count(_)).head as "maxcount").cache()
      crimeWithMonthCast.unpersist()
      val monthMaxCount = crimeGroupByMonth.agg(max("maxcount")).first()
      val monthOfMaxCrime = crimeGroupByMonth.filter(_.getLong(1) == monthMaxCount.getLong(0)).first().getString(0)

      //val currentResult = new Exercise2Result(zipCodeWithMaxCount, zipCodeWithMaxCountCommit, crimeOverTheYear, monthOfMaxCrime) strong type

      val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq((zipCodeWithMaxCount, zipCodeWithMaxCountCommit, crimeOverTheYear, monthOfMaxCrime))))
      df.write.mode(SaveMode.Append).parquet("Exercise2Result.parquet")

      val Exercise2Result_parquet = spark.read.parquet("Exercise2Result.parquet")
      Exercise2Result_parquet.toDF().printSchema()

//      |-- ziodeCodeMostCrime: array (nullable = true)
//      |    |-- element: struct (containsNull = true)
//      |    |    |-- _1: string (nullable = true) [95670,3947]
//      |    |    |-- _2: long (nullable = true)
//      |-- ziodeCodeMostCrimeCommit: array (nullable = true)
//      |    |-- element: struct (containsNull = true) [95670,3947]
//      |    |    |-- _1: string (nullable = true)
//      |    |    |-- _2: long (nullable = true)
//      |-- crimeOverTheYear: long (nullable = true)  35668
//      |-- monthOfMaxCrime: string (nullable = true)  07
      spark.close()
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

  def joinScoreWithAdreess(scoreRdd: RDD[(Long, Double)], addressRdd: RDD[(Long, String)]) : RDD[(Long, (Double, String))] = {
    val joinRdd = scoreRdd.join(addressRdd)
    joinRdd.reduceByKey((x, y) => if(x._1 > y._1) x else y)
  }
}