package com.ostoliarov

import com.ostoliarov.exercise1.Exercise1
import com.ostoliarov.exercise2.Exercise2
import org.apache.spark.sql.SparkSession

object MainApp {
	def main(args: Array[String]) {
		val spark = SparkSession.builder.appName("MFourMobile Big Data Test").getOrCreate()

		println("--------------- Exercise1: Sample data ---------------")
		Exercise1.run(spark, "sample")

		println("--------------- Exercise1: Full data ---------------")
		Exercise1.run(spark, "full")

		println("--------------- Exercise2: Sample data ---------------")
		Exercise2.run(spark, "sample")

		println("--------------- Exercise2: Full data ---------------")
		Exercise2.run(spark, "full")

		spark.stop()
	}
}
