package com.ostoliarov.exercise1

case class Crime(cdatetime: String,
								 address: String,
								 district: String,
								 beat: String,
								 grid: String,
								 crimedescr: String,
								 ucr_ncic_code: Int,
								 latitude: Double,
								 longitude: Double) {}
