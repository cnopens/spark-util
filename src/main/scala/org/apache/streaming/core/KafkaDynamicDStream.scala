package org.apache.streaming.core

import scala.reflect.ClassTag
import org.apache.spark.streaming.kafka.KafkaDataRDD
import kafka.serializer.Decoder
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.scheduler.KafkaRateController

abstract class KafkaDynamicDStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag] {
  val rateController:Option[KafkaRateController] = null    //根据前面批次的执行时间来决定下批次获取多少数据
  var currentOffsets: Map[TopicAndPartition, Long] = null // 记录下次的offset的起点
  
  /**
   * @author LMQ
   * @desc 允许用户修改offset的起点
   */
  def setCurrentOffsets(currentOffsets: Map[TopicAndPartition, Long]) {
    this.currentOffsets = currentOffsets
  }
  
  var computeFunc = (rdd: KafkaDataRDD[K, V, KD, VD, R]) => {
    false //是否立刻执行下个批次
  }
  def foreachRDD(computeFunc: KafkaDataRDD[K, V, KD, VD, R] => Boolean) {
    this.computeFunc = computeFunc
  }
  def batchRDD(): KafkaDataRDD[K, V, KD, VD, R] // 用来获取当前批次的kafkardd
  def generateJob() = computeFunc(batchRDD) //执行job
  def onBatchCompleted():Unit
}
