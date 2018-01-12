package com.microsoft.partnercatalyst.fortis.spark.transforms.entities

import java.io.{IOError, IOException}

import com.microsoft.partnercatalyst.fortis.spark.logging.Loggable
import com.microsoft.partnercatalyst.fortis.spark.transforms.ZipModelsProvider
import com.microsoft.partnercatalyst.fortis.spark.transforms.nlp.OpeNER
import ixa.kaflib.{Entity, KAFDocument, Term}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

@SerialVersionUID(100L)
class EntityRecognizer(
  modelsProvider: ZipModelsProvider,
  language: Option[String]
) extends Serializable with Loggable {

  def extractTerms(text: String): List[Term] = runRecognizer(text, extractTermsUsingModels)
  def extractEntities(text: String): List[Entity] = runRecognizer(text, extractEntitiesUsingModels)
  def isValid: Boolean = language.isDefined && OpeNER.EnabledLanguages.contains(language.get)

  private def runRecognizer[T](text: String, extractor: (String, String) => List[T]): List[T] = {
    if (!isValid) return List()

    Try(modelsProvider.ensureModelsAreDownloaded(language.get)) match {
      case Failure(ex) =>
        logError("Error loading opener model files", ex)
        List()

      case Success(resourcesDirectory) =>
        extractor(text, resourcesDirectory)
    }
  }

  private def extractEntitiesUsingModels(text: String, resourcesDirectory: String): List[Entity] = {
    runOpeNER(text, resourcesDirectory) match {
      case None =>
        logDependency("transforms.entities", "extract.entities", success = false)
        List()

      case Some(kaf) =>
        logDependency("transforms.entities", "extract.entities", success = true)
        kaf.getEntities.toList
    }
  }

  private def extractTermsUsingModels(text: String, resourcesDirectory: String): List[Term] = {
    runOpeNER(text, resourcesDirectory) match {
      case None =>
        logDependency("transforms.entities", "extract.terms", success = false)
        List()

      case Some(kaf) =>
        logDependency("transforms.entities", "extract.terms", success = true)
        kaf.getTerms.toList
    }
  }

  private def runOpeNER(text: String, resourcesDirectory: String): Option[KAFDocument] = {
    var kaf: KAFDocument = null

    try {
      kaf = OpeNER.tokAnnotate(resourcesDirectory, text, language.get)
    } catch {
      case ex @ (_ : NullPointerException | _ : IOError | _ : IOException) =>
        logError(s"Unable to run OpeNER for language $language, failed to tokenize", ex)
        logDependency("transforms.entities", "opener.tokenize", success = false)
        return None
    }

    try {
      OpeNER.posAnnotate(resourcesDirectory, language.get, kaf)
    } catch {
      case ex @ (_ : NullPointerException | _ : IOError | _ : IOException) =>
        logError(s"Unable to run OpeNER extract entities for language $language, failed to part of speech annotate", ex)
        logDependency("transforms.entities", "opener.pos", success = false)
        return None
    }

    try {
      OpeNER.nerAnnotate(resourcesDirectory, language.get, kaf)
    } catch {
      case ex @ (_ : NullPointerException | _ : IOError | _ : IOException) =>
        logError(s"Unable to run OpeNER extract entities for language $language, failed to named entity annotate", ex)
        logDependency("transforms.entities", "opener.ner", success = false)
        return None
    }

    Some(kaf)
  }
}
