package com.microsoft.partnercatalyst.fortis.spark.dto

case class AnalyzedItem[T](
  originalItem: T,
  source: String,
  sharedLocations: List[Location] = List(),
  analysis: Analysis
) extends FortisItem

case class Analysis(
  language: Option[String] = None,
  locations: List[Location] = List(),
  sentiments: List[Double] = List(),
  moods: Set[Tag] = Set(),
  genders: Set[Tag] = Set(),
  keywords: Set[Tag] = Set(),
  entities: Set[Tag] = Set(),
  summary: Option[String] = None
)

case class Location(
  wofId: String,
  confidence: Option[Double] = None,
  latitude: Option[Double] = None,
  longitude: Option[Double] = None
)

case class Tag(
  name: String,
  confidence: Double
)