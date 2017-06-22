package com.microsoft.partnercatalyst.fortis.spark.transforms.people

import com.microsoft.partnercatalyst.fortis.spark.logging.Loggable
import com.microsoft.partnercatalyst.fortis.spark.transforms.entities.EntityRecognizer
import com.microsoft.partnercatalyst.fortis.spark.transforms.nlp.OpeNER
import com.microsoft.partnercatalyst.fortis.spark.transforms.nlp.OpeNER.entityIsPerson

@SerialVersionUID(100L)
class PeopleRecognizer(
  modelsSource: Option[String] = None,
  enabledLanguages: Set[String] = OpeNER.EnabledLanguages
) extends Serializable with Loggable {

  @volatile private lazy val entityRecognizer = createEntityRecognizer()

  def extractPeople(text: String, language: String): List[String] = {
    entityRecognizer.extractEntities(text, language).filter(entityIsPerson).map(_.getStr)
  }

  protected def createEntityRecognizer(): EntityRecognizer = {
    new EntityRecognizer(modelsSource, enabledLanguages)
  }
}