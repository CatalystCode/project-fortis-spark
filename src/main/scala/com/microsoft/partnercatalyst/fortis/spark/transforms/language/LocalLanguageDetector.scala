package com.microsoft.partnercatalyst.fortis.spark.transforms.language

import com.microsoft.partnercatalyst.fortis.spark.logging.Loggable
import com.optimaize.langdetect.LanguageDetectorBuilder
import com.optimaize.langdetect.ngram.NgramExtractors
import com.optimaize.langdetect.profiles.LanguageProfileReader
import com.optimaize.langdetect.text.{CommonTextObjectFactories, TextObjectFactory}

@SerialVersionUID(100L)
class LocalLanguageDetector extends LanguageDetector with Loggable {
  @transient private lazy val languageProfiles = new LanguageProfileReader().readAllBuiltIn
  @transient private lazy val languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard()).withProfiles(languageProfiles).build()
  @transient private lazy val largeTextFactory = CommonTextObjectFactories.forDetectingOnLargeText()
  @transient private lazy val shortTextFactory = CommonTextObjectFactories.forDetectingShortCleanText()

  override def detectLanguage(text: String): Option[String] = {
    if (text.isEmpty) {
      return None
    }

    val language = detectWithFactory(text, if (text.length <= 200) shortTextFactory else largeTextFactory)

    logEvent("transforms.language", Map("success" -> language.isDefined.toString, "detectedLanguage" -> language.getOrElse("")))

    language
  }

  private def detectWithFactory(text: String, factory: TextObjectFactory): Option[String] = {
    Option(languageDetector.detect(factory.forText(text)).orNull).map(_.getLanguage)
  }
}
