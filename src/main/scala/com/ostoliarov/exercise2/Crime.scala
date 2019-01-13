package com.ostoliarov.exercise2

case class Crime(ActivityNumber: String,
								 District: String,
								 Neighborhood: String,
								 OccurenceStartDate: String,
								 OccurenceEndDate: String,
								 ReportDate: String,
								 OccurenceLocation: String,
								 OccurenceCity: String,
								 OccurenceZipCode: Integer,
								 X_Coord: java.lang.Double,
								 Y_Coord: java.lang.Double,
								 PrimaryViolation: String,
								 Injury: String) {}
