package com.microsoft.partnercatalyst.fortis.spark.transforms.sentiment

import java.io.File
import java.util.concurrent.ConcurrentHashMap

import com.microsoft.partnercatalyst.fortis.spark.transforms.ZipModelsProvider
import com.microsoft.partnercatalyst.fortis.spark.transforms.nlp.Tokenizer
import com.microsoft.partnercatalyst.fortis.spark.transforms.sentiment.SentimentDetector.{Negative, Neutral, Positive}

import scala.io.Source
import scala.util.{Failure, Success, Try}

@SerialVersionUID(100L)
class WordListSentimentDetector(
  modelsProvider: ZipModelsProvider,
  language: String
) extends DetectsSentiment {

  @transient private lazy val wordsCache = new ConcurrentHashMap[String, Set[String]]

  def detectSentiment(text: String): Option[Double] = {
    Try(modelsProvider.ensureModelsAreDownloaded(language)) match {
      case Failure(ex) =>
        logError("Error loading sentiment model files", ex)
        None

      case Success(resourcesDirectory) =>
        val words = Tokenizer(text.toLowerCase)
        val numPositiveWords = countPositiveWords(language, words, resourcesDirectory)
        val numNegativeWords = countNegativeWords(language, words, resourcesDirectory)
        computeSentimentScore(numPositiveWords, numNegativeWords)
    }
  }

  private def computeSentimentScore(numPositiveWords: Int, numNegativeWords: Int) = {
    if (numPositiveWords > numNegativeWords) {
      Some(Positive)
    } else if (numNegativeWords > numPositiveWords) {
      Some(Negative)
    } else {
      Some(Neutral)
    }
  }

  private def countNegativeWords(language: String, words: Iterable[String], resourcesDirectory: String) = {
    val negativeWords = readWords(join(resourcesDirectory, s"$language-neg.txt"))
    words.count(negativeWords.contains)
  }

  private def countPositiveWords(language: String, words: Iterable[String], resourcesDirectory: String) = {
    val positiveWords = readWords(join(resourcesDirectory, s"$language-pos.txt"))
    words.count(positiveWords.contains)
  }

  protected def readWords(path: String): Set[String] = {
    val cachedWords = Option(wordsCache.get(path))
    cachedWords match {
      case Some(words) =>
        words

      case None =>
        Try(Source.fromFile(path).getLines().map(_.trim).filter(!_.isEmpty).map(_.toLowerCase).toSet) match {
          case Failure(ex) =>
            logError(s"Error loading sentiment model file $path", ex)
            logDependency("transforms.sentiment", "file.load", success = false)
            Set()

          case Success(words) =>
            logDependency("transforms.sentiment", "file.load", success = true)
            wordsCache.putIfAbsent(path, words)
            words
        }
    }
  }

  private def join(directory: String, filename: String): String = {
    new File(new File(directory), filename).toString
  }
}
