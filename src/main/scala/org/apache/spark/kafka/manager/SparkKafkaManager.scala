package org.apache.spark.streaming.kafka
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkException
import kafka.message.MessageAndMetadata
import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD
import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import scala.collection.mutable.HashMap
import kafka.serializer.Decoder
import scala.reflect.ClassTag
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import java.{ util => ju }
import org.apache.spark.streaming.kafka010.KafkaRDD

object SparkKafkaManager {
  val GROUPID = "group.id"
  val BROKER = "metadata.broker.list"
  val BOOTSTRAP = "bootstrap.servers"
  val SERIALIZER = "serializer.class"
  val VALUE_DESERIALIZER = "value.deserializer"
  val KEY_DESERIALIZER = "key.deserializer"
  val AUTO_COMMIT = "enable.auto.commit"
  val KEY_DESERIALIZER_ENCODE = "key.deserializer.encoding"
  val VALUE_DESERIALIZER_ENCODE = "value.deserializer.encoding"
  val AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset"
  val ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit"
  val RECEIVE_BUFFER_CONFIG = "receive.buffer.bytes"
  /**
   * @author LMQ
   * @time 2018.03.07
   * @description 创建一个 kafkaDataRDD
   * @description 这个kafkaDataRDD是自己定义的，可以自己添加很多自定义的功能（如：更新offset）
   * @description 读取kafka数据有四种方式：
   * @param 1 ： 从最新开始  = LAST
   * 				2 ：从上次消费开始  = CONSUM
   * 				3:从最早的offset开始消费  = EARLIEST
   * 				4：从自定义的offset开始  = CUSTOM
   * @attention maxMessagesPerPartitionKEY：这个参数。是放在sparkconf里面，或者是kp里面。如果两个都没配置，那默认是没有限制，
   * 						这样可能会导致一次性读取的数据量过大。也可以使用另一个带有maxMessagesPerPartition参数的方法来读取
   */
  def createKafkaRDD[K: ClassTag, V: ClassTag](
    sc: SparkContext,
    kafkaParams: Map[String, String],
    offsetRanges: Array[OffsetRange]) = {
      val fixkp=fixKafkaParams(kafkaParams)
    new KafkaRDD[K, V](
      sc, fixkp,
      offsetRanges,
      ju.Collections.emptyMap[TopicPartition, String](),
      true)
  }
  def fixKafkaParams(kafkaParams: Map[String, String]) = {
    val fixKp = new java.util.HashMap[String, Object]()
    kafkaParams.foreach { case (x, y) => fixKp.put(x, y) }
    fixKp.put(ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)
    fixKp.put(AUTO_OFFSET_RESET_CONFIG, "none")
    fixKp.put(RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
    fixKp
  }
}




























