package com.microsoft.partnercatalyst.fortis.spark.transforms.locations

import com.microsoft.partnercatalyst.fortis.spark.IntegrationTestSpec
import com.microsoft.partnercatalyst.fortis.spark.transforms.ZipModelsProvider

class PlaceRecognizerIntegrationSpec extends IntegrationTestSpec {
  "The place recognizer" should "extract correct places" in {
    val localModels = checkIfShouldRunWithLocalModels()
    val modelsProvider = new ZipModelsProvider(
      language => s"https://fortiscentral.blob.core.windows.net/opener/opener-$language.zip",
      localModels)

    val testCases = List(
      ("I went to Paris last week. France was great!", "en", List("France", "Paris")),
      ("A mi me piace Roma.", "it", List("Roma")),
      ("I love Rome.", "en", List("Rome"))
    )

    testCases.foreach(test => {
      val recognizer = new PlaceRecognizer(modelsProvider, Some(test._2))
      val places = recognizer.extractPlacesAndOccurrence(test._1)
      assert(places == test._3)
    })
  }
}
