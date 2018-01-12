package com.microsoft.partnercatalyst.fortis.spark.transforms.locations.client

import com.microsoft.partnercatalyst.fortis.spark.dto.Geofence
import com.microsoft.partnercatalyst.fortis.spark.logging.Loggable
import com.microsoft.partnercatalyst.fortis.spark.transforms.locations.dto.{FeatureServiceFeature, FeatureServiceResponse}
import net.liftweb.json

import scala.io.Source
import scala.util.{Failure, Success, Try}

@SerialVersionUID(100L)
class FeatureServiceClient(apiUrlBase: String, namespace: Option[String]) extends Serializable with Loggable {
  def bbox(geofence: Geofence, layers: Seq[String] = List()): Iterable[FeatureServiceFeature] = {
    unpack(fetchBboxResponse(geofence, layers), "bbox")
  }

  def point(latitude: Double, longitude: Double): Iterable[FeatureServiceFeature] = {
    unpack(fetchPointResponse(latitude = latitude, longitude = longitude), "point")
  }

  def name(names: Iterable[String]): Iterable[FeatureServiceFeature] = {
    unpack(fetchNameResponse(names), "name")
  }

  private def unpack(responseBody: Try[String], endpointName: String): Iterable[FeatureServiceFeature] = {
    val parsedResponse = responseBody.flatMap(parseResponse)
    parsedResponse match {
      case Success(domainObject) =>
        logDependency("transforms.featureservice", endpointName, success = true)
        domainObject
      case Failure(err) =>
        logError(s"Error fetching feature service $endpointName", err)
        logDependency("transforms.featureservice", endpointName, success = false)
        List()
    }
  }

  private def parseResponse(response: String): Try[Iterable[FeatureServiceFeature]] = {
    implicit val formats = json.DefaultFormats

    Try(json.parse(response).extract[FeatureServiceResponse].features)
  }

  protected def fetchBboxResponse(geofence: Geofence, layers: Seq[String]): Try[String] = {
    val fetch = s"$apiUrlBase/features/bbox/${geofence.north}/${geofence.west}/${geofence.south}/${geofence.east}"
    fetchResponse(addQueryParameters(fetch, layers))
  }

  protected def fetchPointResponse(latitude: Double, longitude: Double): Try[String] = {
    val fetch = s"$apiUrlBase/features/point/$latitude/$longitude"
    fetchResponse(addQueryParameters(fetch))
  }

  protected def fetchNameResponse(names: Iterable[String]): Try[String] = {
    val fetch = s"$apiUrlBase/features/name/${names.mkString(",")}"
    fetchResponse(addQueryParameters(fetch))
  }

  private def addQueryParameters(baseUrl: String, layers: Seq[String] = List()): String = {
    var url = baseUrl

    url += "?include=centroid"

    if (layers.nonEmpty) {
      url += s"&filter_layer=${layers.mkString(",")}"
    }

    if (namespace.nonEmpty) {
      url += s"&filter_namespace=${namespace.get}"
    }

    url
  }

  private def fetchResponse(url: String): Try[String] = {
    Try(Source.fromURL(url)("UTF-8").mkString)
  }
}
