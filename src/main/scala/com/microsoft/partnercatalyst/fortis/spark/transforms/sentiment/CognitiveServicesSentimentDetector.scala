package com.microsoft.partnercatalyst.fortis.spark.transforms.sentiment

import net.liftweb.json

import scalaj.http.Http

case class SentimentDetectorAuth(key: String, apiUrlBase: String = "https://westus.api.cognitive.microsoft.com")

@SerialVersionUID(100L)
class CognitiveServicesSentimentDetector(
  language: String,
  auth: SentimentDetectorAuth
) extends DetectsSentiment {

  def detectSentiment(text: String): Option[Double] = {
    val textId = "0"
    val requestBody = buildRequestBody(text, textId)
    val response = callCognitiveServices(requestBody)
    parseResponse(response, textId)
  }

  protected def callCognitiveServices(requestBody: String): String = {
    val response = Http(s"${auth.apiUrlBase}/text/analytics/v2.0/sentiment")
      .headers(
        "Content-Type" -> "application/json",
        "Ocp-Apim-Subscription-Key" -> auth.key)
      .postData(requestBody)
      .asString

    if (response.code != 200) {
      logError(s"Failed to call cognitive services sentiment analysis api: status code ${response.code} with body ${response.body}")
      logDependency("transforms.sentiment", "cognitiveapi", success = false)
    } else {
      logDependency("transforms.sentiment", "cognitiveapi", success = true)
    }

    response.body
  }

  protected def buildRequestBody(text: String, textId: String): String = {
    implicit val formats = json.DefaultFormats
    val requestBody = dto.JsonSentimentDetectionRequest(documents = List(dto.JsonSentimentDetectionRequestItem(
      id = textId,
      language = language,
      text = text)))
    json.compactRender(json.Extraction.decompose(requestBody))
  }

  protected def parseResponse(apiResponse: String, textId: String): Option[Double] = {
    implicit val formats = json.DefaultFormats
    val response = json.parse(apiResponse).extract[dto.JsonSentimentDetectionResponse]
    if (response.errors.exists(_.id == textId)) {
      None
    } else {
      response.documents.find(_.id == textId).map(_.score)
    }
  }
}
