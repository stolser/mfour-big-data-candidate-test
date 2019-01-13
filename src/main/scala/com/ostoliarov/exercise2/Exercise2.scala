package com.ostoliarov.exercise2

import com.ostoliarov.Utils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Encoders, SparkSession}

object Exercise2 {
	private var topNZipCodes: Array[Int] = _

	def run(spark: SparkSession, dataType: String): Unit = {
		import spark.implicits._

		val isTestMode = dataType match {
			case "sample" => true
			case _ => false
		}

		val defaultConfig: Config = ConfigFactory.parseResources("application.conf")
		val crimeBaseFileName = defaultConfig.getString(s"conf.exercise2.data.$dataType.input.crimeBaseFileName")
		val crimeFileExtension = defaultConfig.getString(s"conf.exercise2.data.$dataType.input.crimeFileExtension")
		val mostCommittedCrimesFileName = Utils.randomFileName(
			baseName = defaultConfig.getString(s"conf.exercise2.data.$dataType.output.mostCommittedCrimes")
		)

		val crimeSchema = Encoders.product[Crime].schema
		val crimeMonthlyData = (1 to 12)
			.map(i => f"$crimeBaseFileName$i%02d$crimeFileExtension")
			.map(fileName => spark.read.option("header", true).schema(crimeSchema).csv(fileName).as[Crime])
			.map(_.persist())
			.zip(Stream from 1)
			.map({ case (ds, index) => (index, ds) })
			.toMap

		val crimeFullYearData = crimeMonthlyData.values.reduce(_.union(_))

		def zipCodesWithMostCrimes(): Unit = {
			println("1. Which zip codes had the most crime?")
			val topN = 10
			println(s"Top $topN zip codes with the total number of crimes:")

			val crimesOfTopNZipCodes = crimeFullYearData.groupBy($"OccurenceZipCode")
				.count
				.orderBy($"count".desc)
				.limit(topN)

			crimesOfTopNZipCodes.show

			topNZipCodes = crimesOfTopNZipCodes.select("OccurenceZipCode").as[Int].collect
			println(s"top $topN zip codes = ${topNZipCodes.mkString("[", ",", "]")}")

			if (isTestMode) {
				println("Asserting the results...")

				val actualZipCountPairs = crimesOfTopNZipCodes
					.collect()
					.map(row => (row.getAs[Int]("OccurenceZipCode"), row.getAs[Long]("count")))
					.toSet

				assert(actualZipCountPairs.contains(95670 -> 6))
				assert(actualZipCountPairs.contains(95608 -> 3))
				assert(actualZipCountPairs.contains(95841 -> 2))
				assert(actualZipCountPairs.contains(95820 -> 2))
			}
		}

		def mostCommittedCrimesByZipCode(): Unit = {
			println("\n2. In those zip codes, which was the most committed crime?")
			val mostCommittedCrimes = crimeFullYearData.filter(crime => topNZipCodes.contains(crime.OccurenceZipCode))
				.groupBy($"OccurenceZipCode", $"PrimaryViolation")
				.count
				.orderBy($"OccurenceZipCode".asc, $"count".desc)
				.as[ZipCodeViolationCount]
				.groupByKey(_.OccurenceZipCode)
				.reduceGroups((x, y) => if (x.count > y.count) x else y)
				.map(_._2)
				.orderBy($"count")

			mostCommittedCrimes.foreach(_ match {
				case ZipCodeViolationCount(zipCode, violation, count) => println(f"$zipCode : $violation%65s : $count")
			})

			if (isTestMode) {
				println("Asserting the results...")

				assert(mostCommittedCrimes.collect.contains(ZipCodeViolationCount(95670, "PC 647(F) Disorderly Conduct:Public Intoxication", 4)))
				assert(mostCommittedCrimes.collect.contains(ZipCodeViolationCount(95608, "PC 459 Burglary", 2)))
			}

			println("\nWriting the result to a parquet file...")
			mostCommittedCrimes.write.parquet(mostCommittedCrimesFileName)

			println("Reading the input from the parquet file...")
			val inputFromParquet = spark.read
				.parquet(mostCommittedCrimesFileName)
				.as[ZipCodeViolationCount]
		}

		def howManyTimesCrimesHappen(): Unit = {
			println("\n3. How many times did the crimes happen over the year?")
			val crimesByViolation = crimeFullYearData.groupBy($"PrimaryViolation")
				.count
				.orderBy($"count".desc)
				.collect()
				.map(row => (row.getAs[String]("PrimaryViolation"), row.getAs[Long]("count")))
				.toSet

			crimesByViolation.foreach(item =>
				println(f"${item._1}%65s : ${item._2}")
			)

			if (isTestMode) {
				println("Asserting the results...")

				assert(crimesByViolation.contains(("PC 459 Burglary", 5)))
				assert(crimesByViolation.contains(("PC 647(F) Disorderly Conduct:Public Intoxication", 5)))
				assert(crimesByViolation.contains(("PC 666 Petty Theft W/Prior Jail Term For Specific Offenses", 2)))
				assert(crimesByViolation.contains(("PC 273.5 Inflict Crpl Inj Sp/Cohab", 2)))
				assert(crimesByViolation.contains(("PC 243(D) Battery With Serious Bodily Injury (Punishment)", 2)))
			}
		}

		def numberOfCrimesByMonth(): Unit = {
			println("\n4. What months had the most crime?")
			val monthNames = Map(1 -> "January", 2 -> "February", 3 -> "March", 4 -> "April", 5 -> "May", 6 -> "June",
				7 -> "July", 8 -> "August", 9 -> "September", 10 -> "October", 11 -> "November", 12 -> "December")
			val crimeMonthlyDataSorted = crimeMonthlyData
				.mapValues(_.count)
				.toSeq
				.sortWith({
					case ((index1, count1), (index2, count2)) =>
						if (count1 == count2) index1.compareTo(index2) < 0
						else count1.compareTo(count2) > 0
				})

			crimeMonthlyDataSorted.foreach({
				case (monthIndex, count) => println(f"${monthNames(monthIndex)}%15s: $count")
			})

			if (isTestMode) {
				println("Asserting the results...")

				crimeMonthlyDataSorted.contains(1 -> 2)
				crimeMonthlyDataSorted.contains(2 -> 2)
				crimeMonthlyDataSorted.contains(3 -> 2)
			}
		}

		zipCodesWithMostCrimes()
		mostCommittedCrimesByZipCode()
		howManyTimesCrimesHappen()
		numberOfCrimesByMonth()
	}
}
