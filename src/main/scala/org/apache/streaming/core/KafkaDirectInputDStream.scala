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
 * @desc 不使用sparkstreaming的方式来做实时
 */
class KafkaDirectInputDStream[K: ClassTag, V: ClassTag](
  ssc: StreamingDynamicContext,
  var topics: Set[String])
    extends KafkaDynamicDStream[K, V] {
  
  /**
   * @author LMQ
   * @desc 允许用户修改offset的起点
   */
  def setFromOffsets(fromOffset: Map[TopicAndPartition, Long]){
   this.fromOffset= fromOffset
  }
  def setTopics(topics: Set[String]) = {         //支持用户在执行流式处理的时候，动态地更改topic。//之后会添加更改offset的方法，
    this.topics = topics
  }
  var getKafkaFunc = ()
  /**
   * @author LMQ
   * @desc 获取kafka的RDD，这里如果想自己实现也可以，类似DStream里面的compute
   */
  override def batchRDD() = {
    val kafkardd=ssc.sc.kafkaRDD[K, V](topics,fromOffset)
    fromOffset=kafkardd.getRDDOffsets()
    kafkardd
  }

}
