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
 * @func 不使用sparkstreaming的方式来做实时，而是用while ture的方式来做。
 * 	SparkStreaming有几个问题：1：Job的append，导致数据的延迟处理。
 * 													  2：多余的等待时间，例如batchtime为5s，但是数据处理只用了1s，会导致程序有多余的4s在处于等待。
 * 而使用while true的死循环的方式已经在生产上验证是可行的。其实sparkstreaming也是timer的方式来执行的
 */
class KafkaDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    ssc:SparkKafkaStreamContext[K, V, KD, VD, R],
    msghandle: (MessageAndMetadata[K, V]) => R,
    var topics:Set[String]) {
 
  def getKafkaRDD()={
    ssc.sc.kafkaRDD[K, V, KD, VD, R](topics,msghandle)
  }
  
  var compute = (rdd: KafkaDataRDD[K, V, KD, VD, R]) => {
    
    false //是否有数据
  }
  
  def foreachRDD(compute:KafkaDataRDD[K, V, KD, VD, R] =>Boolean){
    this.compute=compute
  }
  
  def generateJob()={
    val rdd=getKafkaRDD
    compute(rdd)
  }
}