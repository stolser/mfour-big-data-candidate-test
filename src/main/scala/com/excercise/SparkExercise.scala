package com.excercise

import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark._
import org.apache.spark.sql.functions.{collect_list, struct}
import java.io._

object SparkExercise extends Serializable {
  def main(arg: Array[String]) {

    val log = Logger.getLogger(getClass.getName)

    try {
      val sc = SparkSession
        .builder()
        .appName("Exercise1")
        .config("spark.master", "local[*]")
        .getOrCreate()

      val crimeDf = sc.read.format("csv").option("header", "true").load("./src/main/resources/exercise1/SacramentocrimeJanuary2006.csv")

      val uniqueCrime  = crimeDf.select("cdatetime", "address", "crimedescr", "ucr_ncic_code").cache()

      val hourseDf = sc.read.format("csv").option("header", "true").load("./src/main/resources/exercise1/Sacramentorealestatetransactions.csv")
      val uniqueHouse  = hourseDf
                                .filter(hourseDf("sale_date").isNotNull)
                                .filter(hourseDf("price").cast("int") > 0)
                                .withColumnRenamed("street" ,"address")
                                .select("address").cache()

      //1. How many houses were sold?
      val uniqueCrimeCollect = uniqueCrime.count()
      //2. How many crimes occurred?
      val uniqueHouseCollect = uniqueHouse.count()

      //Did any of the crimes occur at one of the houses that were sold?
      //If so:
      //How many?
      //What were the crimes?

      def crimeGroupByHouseAddressDetail: ArrayType = ArrayType(StructType(
        List(
          StructField("cdatetime", StringType, true),
            StructField("crimedescr", StringType, true),
            StructField("ucr_ncic_code", StringType, true)
        )
      ))

      def crimeGroupByHouseAddressSchema: StructType = StructType(
        List(
          StructField("address", StringType, true),
          StructField("detail_list", crimeGroupByHouseAddressDetail)
        )
      )

      val crimeGroupByHouseAddress = uniqueCrime.join(uniqueHouse, "address")
                                        .groupBy("address")
                                        .agg(collect_list(struct("cdatetime", "crimedescr", "ucr_ncic_code"))).collectAsList


      //val crimeGroupByHouseAddressResult = sc.createDataFrame(crimeGroupByHouseAddress, crimeGroupByHouseAddressSchema)

      //lazy, need strong type
      val df = sc.createDataFrame(sc.sparkContext.parallelize(Seq((uniqueCrimeCollect, uniqueHouseCollect,crimeGroupByHouseAddress))))
              .write.mode(SaveMode.Overwrite).parquet("Exercise1Result.parquet")

//      |-- address: string (nullable = true)
//      |-- detail_list: array (nullable = true)
//      |    |-- element: struct (containsNull = true)
//      |    |    |-- cdatetime: string (nullable = true)
//      |    |    |-- crimedescr: string (nullable = true)
//      |    |    |-- ucr_ncic_code: string (nullable = true)
//           2561 19TH AVE   [1/6/06 22:16,12022.1 PC COMMIT FEL ON BAIL,5212]
//                           [1/6/06 22:16,245(A)(1)AWDW/NO FIREARM/CIVIL,1315]
//
//          1900 DANBROOK DR  [1/3/06 15:00,BURGLARY - I RPT,7000]
//                            [1/21/06 20:00,10851(A)VC TAKE VEH W/O OWNER,2404]
//
//         12 COSTA BRASE CT  [1/20/06 0:01,484 PC   PETTY THEFT/INSIDE,2308]
      val crimeGroupByHouseAddress_parquet = sc.read.parquet("Exercise1Result.parquet")
      crimeGroupByHouseAddress_parquet.toDF().printSchema()

      sc.close()

    }
    catch {
      case ex: Exception =>
        log.error("General error", ex)
        throw ex
    }

  }

}