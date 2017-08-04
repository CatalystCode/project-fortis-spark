package com.microsoft.partnercatalyst.fortis.spark.dto

import java.io.ByteArrayInputStream
import java.sql.Timestamp

import org.apache.commons.lang.SerializationUtils.{deserialize, serialize}
import org.joda.time.Instant

object MLModel {

  def dataFrom(modelObj: Serializable): Array[Byte] = {
    serialize(modelObj)
  }

  def dataAs[T](data: Array[Byte]): T = {
    deserialize(new ByteArrayInputStream(data)).asInstanceOf[T]
  }
}

case class MLModel(
  key: String,
  metadata: Map[String, String],
  data: Array[Byte],
  insertiontime: Timestamp = new Timestamp(Instant.now().getMillis)
) extends Serializable {

  def dataAs[T](): T = MLModel.dataAs(this.data)

}
