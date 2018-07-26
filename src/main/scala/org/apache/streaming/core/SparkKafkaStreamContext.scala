package org.apache.streaming.core
import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.streaming.Duration

/**
 * 与StreamingKafkaContext不同，这里不使用SparkStreaming来实现流的功能，而是用while true来实现
 */
class SparkKafkaStreamContext {
  var sc: SparkKafkaContext = null
  var batchDuration: Duration = null
  def this(sc: SparkKafkaContext, batchDuration: Duration) {
    this()
    this.sc = sc
    this.batchDuration = batchDuration
  }
  def createKafkaStreamRDD(
    kp:     Map[String, String],
    topics: Set[String]) = {
    val getRDDFunc = {
      sc.kafkaRDD(kp, topics)
    }
    new KafkaDirectStreamRDD(getRDDFunc, batchDuration)
  }
}