package com.ostoliarov.exercise1

import com.ostoliarov.Utils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object Exercise1 {
	private var crimesJoinedTransactions: Dataset[(Crime, RealEstateTransaction)] = _
	private var occurredCrimes: DataFrame = _

	def run(spark: SparkSession, dataType: String): Unit = {
		import spark.implicits._

		val isTestMode = dataType match {
			case "sample" => true
			case _ => false
		}

		val defaultConfig: Config = ConfigFactory.parseResources("application.conf")
		val crimeFileName = defaultConfig.getString(s"conf.exercise1.data.$dataType.input.crimeFile")
		val transactionFileName = defaultConfig.getString(s"conf.exercise1.data.$dataType.input.transactionFile")
		val outputCrimeTransFileName = Utils.randomFileName(
			baseName = defaultConfig.getString(s"conf.exercise1.data.$dataType.output.outputCrimeTransFile")
		)

		val crimeSchema = Encoders.product[Crime].schema
		val transactionSchema = Encoders.product[RealEstateTransaction].schema

		val crimeDf = spark.read.option("header", true).schema(crimeSchema).csv(crimeFileName)
		val crimeDs = crimeDf.as[Crime].persist()
		val crimeDsGeo = crimeDs.map(crime => (s"${crime.latitude}${crime.longitude}", crime)) // (unique_geo_location, crime)

		val transactionDf = spark.read.option("header", true).schema(transactionSchema).csv(transactionFileName)
		val transactionDs = transactionDf.as[RealEstateTransaction].persist()
		val transactionDsGeo = transactionDs.map(trans => (s"${trans.latitude}${trans.longitude}", trans)) // (unique_geo_location, transaction)

		def howManyHousesWereSold(): Unit = {
			println("1. How many houses were sold?")
			// - the number of transactions:
			val transNumber = transactionDs.count
			if (isTestMode) assert(transNumber == 22)
			println(s"The number of transactions: $transNumber")

			// - the number of unique houses:
			val uniqueTransNumber = transactionDs.map(t => s"${t.latitude}${t.longitude}").distinct.count
			println(s"The number of unique transactions: $uniqueTransNumber\n")

			if (isTestMode) assert(uniqueTransNumber == 20)
		}

		def howManyCrimesOccurred(): Unit = {
			println("2. How many crimes occurred?")
			val crimeNumber = crimeDs.count
			println(s"The number of crimes: $crimeNumber\n")

			if (isTestMode) assert(crimeNumber == 10)
		}

		def crimesOccurredAtSoldHouses(): Unit = {
			println("3. Did any of the crimes occur at one of the houses that were sold?")

			crimesJoinedTransactions = if (isTestMode) {
				// the sample data is joined on the unique geo location:
				crimeDsGeo
					.joinWith(transactionDsGeo, crimeDsGeo.col("_1") === transactionDsGeo.col("_1"))
					.map(x => (x._1._2, x._2._2))
			} else {
				// the full data is joined on 'address' == 'street':
				crimeDs
					.joinWith(transactionDs, crimeDs.col("address") === transactionDs.col("street"))
			}

			crimesJoinedTransactions.persist()
			val crimesAtHousesNumber = crimesJoinedTransactions.count
			println(s"The number of crimes that occurred at the sold houses: $crimesAtHousesNumber\n")
		}

		def whatWereTheCrimes(): Unit = {
			occurredCrimes = crimesJoinedTransactions
				.map({ case (crime, trans) => (trans.street, trans.city, trans.`type`,
					trans.sq__ft, crime.cdatetime, crime.crimedescr)
				})
				.selectExpr("_1 as street", "_2 as city", "_3 as estateType", "_4 as squareFt",
					"_5 as crimeDateTime", "_6 as crimeDescription")

			println("4. What were the crimes?")
			println("The list of unique crimes:")
			occurredCrimes.select("crimeDescription")
				.collect
				.distinct
				.foreach(println)
		}

		def extraQuestions(): Unit = {
			println("\n ---- Extra questions ----")
			println("The number of 'BURGLARY' crimes:")
			val burglaryCrimes = crimeDs
				.filter(_.crimedescr.contains("BURGLARY"))
				.groupByKey(_.crimedescr)
				.mapGroups({ case (description, crimes) => (description, crimes.size) })

			burglaryCrimes.collect.foreach(println)

			println("\nAverage price and max square of the real estate with price > 100 000:")
			transactionDs.filter("price > 100000")
				.groupBy($"`type`", $"beds")
				.agg(Map(
					"price" -> "avg",
					"sq__ft" -> "max"))
				.show

			println("The number, min/average/max price of 'Residential' houses by the number of beds:")
			transactionDs.filter("`type` == 'Residential'")
				.groupBy($"beds")
				.agg(count($"price"), min($"price"), avg($"price"), max($"price"))
				.show

			println("Join data using Spark SQL:")
			crimeDs.createOrReplaceTempView("crimes")
			transactionDs.createOrReplaceTempView("transactions")
			spark.sql("SELECT crimes.cdatetime, transactions.street, transactions.price " +
				"FROM crimes JOIN transactions ON crimes.address == transactions.street ")
				.show

		}

		howManyHousesWereSold()
		howManyCrimesOccurred()
		crimesOccurredAtSoldHouses()
		whatWereTheCrimes()

		println("\nWriting the result to a parquet file...")
		occurredCrimes.write.parquet(outputCrimeTransFileName)

		println("Reading the input from the parquet file...")
		val inputFromParquet = spark.read
			.parquet(outputCrimeTransFileName)
			.as[CrimesJoinedTransactionsResult]

		println("The data from the parquet file:")
		inputFromParquet.take(20).foreach(println)

		extraQuestions()

		println("=================================================================================")
	}
}
