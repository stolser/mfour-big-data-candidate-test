package com.ostoliarov.exercise1

case class RealEstateTransaction(street: String,
																 city: String,
																 zip: Int,
																 state: String,
																 beds: Int,
																 baths: Int,
																 sq__ft: Int,
																 `type`: String,
																 sale_date: String,
																 price: Long,
																 latitude: Double,
																 longitude: Double) {}