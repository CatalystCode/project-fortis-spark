package com.microsoft.partnercatalyst.fortis.spark.dto

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Date

object MLModel {

  def dataFrom(modelObj: Serializable): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(modelObj)
    oos.flush()
    oos.close()
    stream.toByteArray
  }

  def dataAs[T](data: Array[Byte]): T = {
    new ObjectInputStream(
      new ByteArrayInputStream(data)
    )
      .readObject()
      .asInstanceOf[T]
  }
}

case class MLModel(key: String,
                   metadata: Map[String, String],
                   data: Array[Byte],
                   insertiontime: Long = new Date().getTime) extends Serializable {

  def dataAs[T](): T = MLModel.dataAs(this.data)

}
