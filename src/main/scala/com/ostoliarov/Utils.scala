package com.ostoliarov

import scala.util.Random

object Utils {
	def randomFileName(baseName: String): String = {
		val letters = ('a' to 'z').toList
		val randomName = (1 to 5).map(_ => letters(Random.nextInt(26))).mkString
		s"${baseName}_$randomName"
	}
}
