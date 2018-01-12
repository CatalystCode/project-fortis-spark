package com.microsoft.partnercatalyst.fortis.spark.transforms

import java.io.{File, FileNotFoundException, IOError}
import java.net.URL
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap

import com.microsoft.partnercatalyst.fortis.spark.logging.Loggable
import net.lingala.zip4j.core.ZipFile
import net.lingala.zip4j.exception.ZipException

import scala.collection.JavaConversions._
import scala.sys.process._

@SerialVersionUID(100L)
class ZipModelsProvider(
  modelsUrlFromLanguage: String => String,
  modelsSource: Option[String] = None
) extends Serializable with Loggable {

  @transient private lazy val modelDirectories = new ConcurrentHashMap[String, String]

  def ensureModelsAreDownloaded(language: String): String = {
    try {
      val localPath = modelsSource.getOrElse("")
      if (hasModelFiles(localPath, language)) {
        logDebug(s"Using locally provided model files from $localPath")
        modelDirectories.putIfAbsent(language, localPath)
        return localPath
      }

      val previouslyDownloadedPath = modelDirectories.getOrElse(language, "")
      if (hasModelFiles(previouslyDownloadedPath, language)) {
        logDebug(s"Using previously downloaded model files from $previouslyDownloadedPath")
        return previouslyDownloadedPath
      }

      val remotePath = modelsSource.getOrElse(modelsUrlFromLanguage(language))
      if ((!remotePath.startsWith("http://") && !remotePath.startsWith("https://")) || !remotePath.endsWith(".zip")) {
        throw new FileNotFoundException(s"Unable to process $remotePath, should be http(s) link to zip file")
      }

      val localDir = downloadModels(remotePath)
      if (!hasModelFiles(localDir, language)) {
        throw new FileNotFoundException(s"No models for language $language in $remotePath")
      }

      logDependency("transforms.models", "ensuredownloaded", success = true)
      modelDirectories.putIfAbsent(language, localDir)
      localDir
    } catch {
      case ex @ (_ : ZipException | _ : IOError) =>
        logError(s"Error downloading models from ${modelsUrlFromLanguage(language)}", ex)
        logDependency("transforms.models", "ensuredownloaded", success = false)
        throw ex
    }
  }

  private def downloadModels(remotePath: String): String = {
    val localFile = Files.createTempFile(getClass.getSimpleName, ".zip").toAbsolutePath.toString
    val localDir = Files.createTempDirectory(getClass.getSimpleName).toAbsolutePath.toString
    val exitCode = (new URL(remotePath) #> new File(localFile)).!
    if (exitCode != 0) {
      throw new FileNotFoundException(s"Unable to download models from $remotePath")
    }
    new ZipFile(localFile).extractAll(localDir)
    new File(localFile).delete()
    localDir
  }

  private def hasModelFiles(modelsDir: String, language: String): Boolean = {
    if (modelsDir.isEmpty) return false

    val modelFiles = new File(modelsDir).listFiles
    modelFiles != null && modelFiles.exists(_.getName.startsWith(s"$language-"))
  }
}
