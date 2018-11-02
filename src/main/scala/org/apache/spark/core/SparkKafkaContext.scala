package org.apache.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import kafka.serializer.StringDecoder
import org.apache.spark.kafka.util.KafkaCluster
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
import org.apache.spark.kafka.util.KafkaCluster
import org.apache.spark.streaming.kafka010.HasOffsetRanges

/**
 * @author LMQ
 * @description 用于替代SparkContext。但sparkContext的很多功能没写，可以自己添加，或者直接拿sparkcontext使用
 * @description 此类主要是用于 创建 kafkaRDD 。
 * @description 创建的kafkaRDD提供更新偏移量的能力
 */
class SparkKafkaContext[K, V] {
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
  var sparkcontext: SparkContext = null
  var kc: KafkaCluster[K, V] = null
  lazy val conf = sparkcontext.getConf
  def initKafkaCluster(kp: Map[String, String]) = {
    if (kc == null) {
      kc = new KafkaCluster(kp)
    }
  }

  def this(kp: Map[String, String], sparkcontext: SparkContext) {
    this()
    initKafkaCluster(kp)
    this.sparkcontext = sparkcontext
  }
  def this(kp: Map[String, String], conf: SparkConf) {
    this()
    initKafkaCluster(kp)
    sparkcontext = new SparkContext(conf)
  }
  def this(kp: Map[String, String], master: String, appName: String) {
    this()
    val conf = new SparkConf()
    initKafkaCluster(kp)
    conf.setMaster(master)
    conf.setAppName(appName)
    sparkcontext = new SparkContext(conf)
  }
  def this(kp: Map[String, String], appName: String) {
    this()
    initKafkaCluster(kp)
    val conf = new SparkConf()
    conf.setAppName(appName)
    sparkcontext = new SparkContext(conf)
  }
  def broadcast[T: ClassTag](value: T) = {
    sparkcontext.broadcast(value)
  }

  /**
   * @author LMQ
   * @time 2018.11.01
   */
  def createKafkaRDD[K: ClassTag, V: ClassTag](offsetRanges: Array[OffsetRange]) = {
    new KafkaRDD[K, V](
      sparkcontext,
      kc.fixKp,
      offsetRanges,
      ju.Collections.emptyMap[TopicPartition, String](),
      true)
  }
  def createKafkaRDD[K: ClassTag, V: ClassTag](topics: Set[String]) = {
    new KafkaRDD[K, V](
      sparkcontext,
      kc.fixKafkaExcutorParams(),
      kc.getOffsetRange(topics),
      ju.Collections.emptyMap[TopicPartition, String](),
      true)
  }
  def createKafkaRDD[K: ClassTag, V: ClassTag](topics: Set[String], perParLimit: Long) = {
    new KafkaRDD[K, V](
      sparkcontext,
      kc.fixKafkaExcutorParams(),
      kc.getOffsetRange(topics, perParLimit),
      ju.Collections.emptyMap[TopicPartition, String](),
      true)
  }
  
  def updateOffset[T](rdd:RDD[T]){
    val untilOffset=rdd
    .asInstanceOf[HasOffsetRanges]
    .offsetRanges
    .map { x => (x.topicPartition(),x.untilOffset) }.toMap
    kc.updateOffset(untilOffset)
  }
  
 def updateOffset(offsetRanges: Array[OffsetRange]){
    val untilOffset=
    offsetRanges
    .map { x => (x.topicPartition(),x.untilOffset) }.toMap
    kc.updateOffset(untilOffset)
  }
  
}
