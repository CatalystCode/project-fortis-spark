package com.microsoft.partnercatalyst.fortis.spark.transforms.image

import com.microsoft.partnercatalyst.fortis.spark.dto.{Analysis, Location, Tag}
import com.microsoft.partnercatalyst.fortis.spark.transforms.image.dto.JsonImageLandmark
import org.scalatest.FlatSpec

class TestImageAnalyzer(cognitiveServicesResponse: String) extends ImageAnalyzer(ImageAnalysisAuth("key"), null) {
  override protected def callCognitiveServices(requestBody: String): String = cognitiveServicesResponse
  override def buildRequestBody(imageUrl: String): String = super.buildRequestBody(imageUrl)
  override def landmarksToLocations(landmarks: Iterable[JsonImageLandmark]): Iterable[Location] = landmarks.map(x => Location(x.name, Some(x.confidence)))
}

class ImageAnalyzerSpec extends FlatSpec {
  "The image analyzer" should "produce domain objects from the json api response" in {
    val response = new TestImageAnalyzer(
      """
        |{
        |  "categories": [
        |    {
        |      "name": "abstract_",
        |      "score": 0.00390625
        |    },
        |    {
        |      "name": "people_",
        |      "score": 0.83984375,
        |      "detail": {
        |        "celebrities": [
        |          {
        |            "name": "Satya Nadella",
        |            "faceRectangle": {
        |              "left": 597,
        |              "top": 162,
        |              "width": 248,
        |              "height": 248
        |            },
        |            "confidence": 0.999028444
        |          }
        |        ],
        |        "landmarks":[
        |          {
        |            "name":"Forbidden City",
        |            "confidence": 0.9978346
        |          }
        |        ]
        |      }
        |    }
        |  ],
        |  "adult": {
        |    "isAdultContent": false,
        |    "isRacyContent": false,
        |    "adultScore": 0.0934349000453949,
        |    "racyScore": 0.068613491952419281
        |  },
        |  "tags": [
        |    {
        |      "name": "person",
        |      "confidence": 0.98979085683822632
        |    },
        |    {
        |      "name": "man",
        |      "confidence": 0.94493889808654785
        |    },
        |    {
        |      "name": "outdoor",
        |      "confidence": 0.938492476940155
        |    },
        |    {
        |      "name": "window",
        |      "confidence": 0.89513939619064331
        |    }
        |  ],
        |  "description": {
        |    "tags": [
        |      "person",
        |      "man",
        |      "outdoor",
        |      "window",
        |      "glasses"
        |    ],
        |    "captions": [
        |      {
        |        "text": "Satya Nadella sitting on a bench",
        |        "confidence": 0.48293603002174407
        |      }
        |    ]
        |  },
        |  "requestId": "0dbec5ad-a3d3-4f7e-96b4-dfd57efe967d",
        |  "metadata": {
        |    "width": 1500,
        |    "height": 1000,
        |    "format": "Jpeg"
        |  },
        |  "faces": [
        |    {
        |      "age": 44,
        |      "gender": "Male",
        |      "faceRectangle": {
        |        "left": 593,
        |        "top": 160,
        |        "width": 250,
        |        "height": 250
        |      }
        |    }
        |  ],
        |  "color": {
        |    "dominantColorForeground": "Brown",
        |    "dominantColorBackground": "Brown",
        |    "dominantColors": [
        |      "Brown",
        |      "Black"
        |    ],
        |    "accentColor": "873B59",
        |    "isBWImg": false
        |  },
        |  "imageType": {
        |    "clipArtType": 0,
        |    "lineDrawingType": 0
        |  }
        |}
      """.stripMargin).analyze("http://test.com/image.png")

    assert(response === Analysis(
      keywords = Set(Tag("person", 0.98979085683822632), Tag("man", 0.94493889808654785), Tag("outdoor", 0.938492476940155), Tag("window", 0.89513939619064331)),
      summary = Some("Satya Nadella sitting on a bench"),
      entities = Set(Tag("Satya Nadella", 0.999028444)),
      locations = List(Location(wofId = "Forbidden City", confidence = Some(0.9978346)))
    ))
  }

  it should "empty out optional fields" in {
    val response = new TestImageAnalyzer(
      """
        |{
        |  "categories": [
        |    {
        |      "name": "abstract_",
        |      "score": 0.00390625
        |    },
        |    {
        |      "name": "people_",
        |      "score": 0.83984375
        |    }
        |  ],
        |  "adult": {
        |    "isAdultContent": false,
        |    "isRacyContent": false,
        |    "adultScore": 0.0934349000453949,
        |    "racyScore": 0.068613491952419281
        |  },
        |  "tags": [
        |    {
        |      "name": "person",
        |      "confidence": 0.98979085683822632
        |    },
        |    {
        |      "name": "man",
        |      "confidence": 0.94493889808654785
        |    },
        |    {
        |      "name": "outdoor",
        |      "confidence": 0.938492476940155
        |    },
        |    {
        |      "name": "window",
        |      "confidence": 0.89513939619064331
        |    }
        |  ],
        |  "description": {
        |    "tags": [
        |      "person",
        |      "man",
        |      "outdoor",
        |      "window",
        |      "glasses"
        |    ]
        |  },
        |  "requestId": "0dbec5ad-a3d3-4f7e-96b4-dfd57efe967d",
        |  "metadata": {
        |    "width": 1500,
        |    "height": 1000,
        |    "format": "Jpeg"
        |  },
        |  "faces": [
        |    {
        |      "age": 44,
        |      "gender": "Male",
        |      "faceRectangle": {
        |        "left": 593,
        |        "top": 160,
        |        "width": 250,
        |        "height": 250
        |      }
        |    }
        |  ],
        |  "color": {
        |    "dominantColorForeground": "Brown",
        |    "dominantColorBackground": "Brown",
        |    "dominantColors": [
        |      "Brown",
        |      "Black"
        |    ],
        |    "accentColor": "873B59",
        |    "isBWImg": false
        |  },
        |  "imageType": {
        |    "clipArtType": 0,
        |    "lineDrawingType": 0
        |  }
        |}
      """.stripMargin).analyze("http://test.com/image.png")

    assert(response === Analysis(
      keywords = Set(Tag("person", 0.98979085683822632), Tag("man", 0.94493889808654785), Tag("outdoor", 0.938492476940155), Tag("window", 0.89513939619064331)),
      summary = None,
      entities = Set(),
      locations = List()
    ))
  }

  it should "build correct request body" in {
    val imageUrl = "http://test.com/image.png"
    val requestBody = new TestImageAnalyzer("").buildRequestBody(imageUrl)

    assert(requestBody == s"""{"url":"$imageUrl"}""")
  }
}
