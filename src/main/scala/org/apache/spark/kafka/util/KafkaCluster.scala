package org.apache.spark.kafka.util

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.kafka010.OffsetRange
class KafkaCluster[K, V](kp: Map[String, String]) {

  lazy val fixKp = fixKafkaParams(kp)
  var excutorFixKp: java.util.HashMap[String, Object] = null
  @transient private var kc: Consumer[K, V] = null

  /**
   * @author LMQ
   * @time 2018-10-31
   * @desc 获取consumer
   */
  def c(): Consumer[K, V] = this.synchronized {
    if (null == kc) {
      kc = new KafkaConsumer[K, V](fixKp)
    }
    kc
  }
  /**
   * @author LMQ
   * @time 2018-10-31
   * @desc 关闭consumer
   */
  def close() {
    if (kc != null) {
      kc.close()
      kc = null
    }
  }
  /**
   * @author LMQ
   * @time 2018-10-31
   * @desc 修正kp的配置
   */
  def fixKafkaParams(kafkaParams: Map[String, String]) = {
    val fixKp = new java.util.HashMap[String, Object]()
    kafkaParams.foreach { case (x, y) => fixKp.put(x, y) }
    fixKp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)
    if (!fixKp.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) || fixKp.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) == "none")
      fixKp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    fixKp.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
    fixKp
  }

  /**
   * @author LMQ
   * @time 2018-10-31
   * @desc 修正kp的配置。当使用spark读取kafka数据的时候，ssl配置文件的路径不能写全路径（除非每台node下都有这个路径）。建议使用--files 来上传ssl配置。这里提供了修正一开始的kp
   */
  def fixKafkaExcutorParams() = {
    if (excutorFixKp == null) {
      excutorFixKp = new java.util.HashMap[String, Object]()
      fixKp.foreach { case (x, y) => excutorFixKp.put(x, y) }
      excutorFixKp.put("ssl.truststore.location", "client.truststore.jks")
      excutorFixKp.put("ssl.keystore.location", "client.keystore.jks")
    }
    excutorFixKp
  }

  /**
   * @author LMQ
   * @time 2018-10-31
   * @desc 获取上次消费的offset
   */
  def getConsumerOffet(topics: Set[String]) = {
    c.subscribe(topics)
    c.poll(0)
    val parts = c.assignment()
    parts.map { tp => tp -> c.position(tp) }.toMap
  }
  /**
   * @author LMQ
   * @time 2018-10-31
   * @desc 更新偏移量
   */
  def updateOffset(offset: Map[TopicPartition, Long]) {
    offset.foreach { case (tp, l) => c.seek(tp, l) }
    c.commitAsync()
  }
  /**
   * @author LMQ
   * @time 2018-10-31
   * @desc 获取最新偏移量
   */
  def getLastestOffset(topics: Set[String]) = {
    c.subscribe(topics)
    c.poll(0)
    val parts = c.assignment()
    val currentOffset = parts.map { tp => tp -> c.position(tp) }.toMap
    c.pause(parts)
    c.seekToEnd(parts)
    val re = parts.map { ps => ps -> c.position(ps) }
    currentOffset.foreach { case (tp, l) => c.seek(tp, l) }
    re.toMap
  }
  /**
   * @author LMQ
   * @time 2018-11-02
   * @desc 读取数据的范围，默认为 ： 上次消费到最新数据
   */
  def getOffsetRange(topics: Set[String], perPartMaxNum: Long = 10000) = {
    val consumerOffset = getConsumerOffet(topics)
    val lastOffset = getLastestOffset(topics)
    val earlestOffset = getEarleastOffset(topics)
    lastOffset.map {
      case (tp, l) =>
        if (consumerOffset.contains(tp)) {
          val untilOff =  if(earlestOffset.contains(tp)){
            if(consumerOffset(tp)<earlestOffset(tp)){//过期
              if (perPartMaxNum > 0) Math.min(earlestOffset(tp), l) else l
            }else{
              if (perPartMaxNum > 0) Math.min(consumerOffset(tp), l) else l
            }
          }else{
            if (perPartMaxNum > 0) Math.min(consumerOffset(tp), l) else l
          }
          OffsetRange.create(tp.topic, tp.partition, consumerOffset(tp), untilOff)
        } else {
          val untilOff = if (perPartMaxNum > 0) Math.min(perPartMaxNum, l) else l
          OffsetRange.create(tp.topic, tp.partition, 0, untilOff)
        }
    }.toArray
  }
  /**
   * @author LMQ
   * @time 2018-10-31
   * @desc 获取最开始偏移量
   */
  def getEarleastOffset(topics: Set[String]) = {
    c.subscribe(topics)
    c.poll(0)
    val parts = c.assignment()
    val currentOffset = parts.map { tp => tp -> c.position(tp) }.toMap
    c.pause(parts)
    c.seekToBeginning(parts)
    val re = parts.map { ps => ps -> c.position(ps) }
    currentOffset.foreach { case (tp, l) => c.seek(tp, l) }
    re.toMap
  }
}