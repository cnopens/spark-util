package org.apache.streaming.core
import kafka.message.MessageAndMetadata
import scala.reflect.ClassTag
import kafka.serializer.Decoder
import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import java.util.Date
import org.apache.spark.streaming.kafka.KafkaDataRDD

/**
 * @author LinMingQiang
 * @time 2018-07-07
 * @func 不使用sparkstreaming的方式来做实时
 */
class KafkaDirectInputDStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
  ssc: StreamingDynamicContext,
  msghandle: (MessageAndMetadata[K, V]) => R,
  var topics: Set[String])
    extends KafkaDynamicDStream[K, V, KD, VD, R] {
  def setTopics(topics: Set[String]) = {
    this.topics = topics
  }
  var getKafkaFunc = ()
  /**
   * @author LMQ
   * @desc 获取kafka的RDD，这里如果想自己实现也可以，类似DStream里面的compute
   */
  override def batchRDD() = {
    ssc.sc.kafkaRDD[K, V, KD, VD, R](topics, msghandle)
  }

}