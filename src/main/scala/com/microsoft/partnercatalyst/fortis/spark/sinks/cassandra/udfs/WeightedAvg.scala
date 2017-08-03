package com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.udfs

import org.apache.spark.sql.{Row}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

case class Average(var sum: Float, var count: BigInt)

abstract class WeightedAvg extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("avgsentiment", FloatType) :: StructField("mentioncount", LongType) :: Nil)
  override def bufferSchema: StructType = {
    StructType(StructField("sum", FloatType) :: StructField("count", LongType) :: Nil)
  }

  override def dataType: DataType = FloatType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0F
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row)

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row)

  // Calculates the final result
  override def evaluate(buffer: Row): Float

  def getFloatValue(floatValue: Option[Float]): Float = {
    floatValue match {
      case None => 0
      case Some(number) => number
    }
  }

  def getLongValue(longValue: Option[Long]): Long = {
    longValue match {
      case None => 0
      case Some(number) => number
    }
  }
}

/*abstract class WeightedAvg extends Aggregator[GenericRowWithSchema, Average, Float] {
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    def zero: Average = Average(0f, BigInt(0))
    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    def reduce(buffer: Average, record: GenericRowWithSchema): Average

    // Merge two intermediate values
    def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }
    // Transform the output of the reduction
    def finish(reduction: Average): Float = reduction.sum / reduction.count.toFloat
    // Specifies the Encoder for the intermediate value type
    def bufferEncoder: Encoder[Average] = Encoders.product
    // Specifies the Encoder for the final output value type
    def outputEncoder: Encoder[Float] = Encoders.scalaFloat
    def getFloatValue(floatValue: Float): Float = {
      Option(floatValue) match {
        case None => 0
        case Some(number) => floatValue
      }
    }

    def getLongValue(longValue: Long): Long = {
      Option(longValue) match {
        case None => 0
        case Some(number) => longValue
      }
    }
}
*/
object SentimentWeightedAvg extends WeightedAvg with Serializable{
   /*override def reduce(buffer: Average, record: GenericRowWithSchema): Average = {
     val sentiment = getFloatValue(record.getAs[Float]("avgsentiment"))
     val mentioncount = getLongValue(record.getAs[Long]("mentioncount"))
     buffer.sum += sentiment * mentioncount
     buffer.count += mentioncount
     buffer
   }*/

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val sentiment = getFloatValue(Option(input.getFloat(0)))
    val count = getLongValue(Option(input.getLong(1)))
    val currentSentiment = buffer.getFloat(0)
    val currentCount = buffer.getLong(1)

    buffer.update(0, currentSentiment + (sentiment * count.toFloat))
    buffer.update(1, currentCount + count)
  }

  override def evaluate(buffer: Row): Float = {
    getFloatValue(Option(buffer.getFloat(0))) / getLongValue(Option(buffer.getLong(1))).toFloat
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val sentiment = getFloatValue(Option(buffer2.getFloat(0)))
    val count = getLongValue(Option(buffer2.getLong(1)))
    val currentSentiment = getFloatValue(Option(buffer1.getFloat(0)))
    val currentCount = getLongValue(Option(buffer1.getLong(1)))

    buffer1.update(0, currentSentiment + sentiment)
    buffer1.update(1, currentCount + count)
  }
}