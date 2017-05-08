package com.microsoft.partnercatalyst.fortis.spark.transforms.locations

import java.io.{BufferedReader, ByteArrayInputStream, File, InputStreamReader}
import java.util.Properties

import eus.ixa.ixa.pipe.nerc.{Annotate => NerAnnotate}
import eus.ixa.ixa.pipe.pos.{Annotate => PosAnnotate}
import eus.ixa.ixa.pipe.tok.{Annotate => TokAnnotate}
import ixa.kaflib.KAFDocument

import scala.collection.JavaConversions._

@SerialVersionUID(100L)
class PlaceRecognizer(
  modelsDirectory: String
) extends Serializable {

  private lazy val supportedLanguages = Set("de", "en", "es", "eu", "fr", "gl", "it", "nl")

  def extractPlaces(text: String, language: String): Iterable[String] = {
    if (!supportedLanguages.contains(language)) {
      return Set()
    }

    val kaf = new KAFDocument(language, "v1.naf")
    createTokAnnotate(language, text).tokenizeToKAF(kaf)
    createPosAnnotate(language).annotatePOSToKAF(kaf)
    createNerAnnotate(language).annotateNEs(kaf)

    kaf.getEntities.toList.filter(_.getType == "LOCATION").map(_.getStr).toSet
  }

  private def createTokAnnotate(language: String, text: String): TokAnnotate = {
    val properties = new Properties
    properties.setProperty("language", language)
    properties.setProperty("resourcesDirectory", modelsDirectory)
    properties.setProperty("normalize", "default")
    properties.setProperty("untokenizable", "no")
    properties.setProperty("hardParagraph", "no")

    val input = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(text.getBytes("UTF-8"))))
    new TokAnnotate(input, properties)
  }

  private def createPosAnnotate(language: String): PosAnnotate = {
    val properties = new Properties
    properties.setProperty("language", language)
    properties.setProperty("model", new File(modelsDirectory, s"$language-pos.bin").getAbsolutePath)
    properties.setProperty("lemmatizerModel", new File(modelsDirectory, s"$language-lemmatizer.bin").getAbsolutePath)
    properties.setProperty("resourcesDirectory", modelsDirectory)
    properties.setProperty("multiwords", "false")
    properties.setProperty("dictag", "false")

    new PosAnnotate(properties)
  }

  private def createNerAnnotate(language: String): NerAnnotate = {
    val properties = new Properties
    properties.setProperty("language", language)
    properties.setProperty("model", new File(modelsDirectory, s"$language-nerc.bin").getAbsolutePath)
    properties.setProperty("ruleBasedOption", "off")
    properties.setProperty("dictTag", "off")
    properties.setProperty("dictPath", "off")
    properties.setProperty("clearFeatures", "no")

    new NerAnnotate(properties)
  }
}
