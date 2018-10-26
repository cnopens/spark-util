package com.test

import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.core.SparkKafkaConfsKey
import java.util.Properties
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka.SparkKafkaManager
object Test extends SparkKafkaConfsKey {
  def main(args: Array[String]): Unit = {
    val brokers = "kafka-1:9092,kafka-3:9092,kafka-3:9092"
    val groupId = "test"
    val topic = "smartadsclicklog"
    val topics = Set("smartadsclicklog")
    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    val props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("group.id", "test");
    props.put("enable.auto.commit", "false"); //自动commit
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("key.deserializer.encoding", "UTF8");
    props.put("value.deserializer.encoding", "UTF8");
    val offsetRanges: Array[OffsetRange] = Array(OffsetRange.create(topic, 0, 134380, 134382),
      OffsetRange.create(topic, 1, 134381, 134382),
      OffsetRange.create(topic, 2, 134383, 134389),
      OffsetRange.create(topic, 3, 134381, 134389),
      OffsetRange.create(topic, 4, 134390, 134399),
      OffsetRange.create(topic, 5, 134390, 134399))
    val rdd=SparkKafkaManager.createKafkaRDD(sc, props.toMap, offsetRanges)
    rdd.foreach(x=>println(x.value()))
    //KafkaUtils.createRDD[String,String](sc, kp, offsetRanges, java.util.Collections.emptyMap[TopicPartition, String]())
    //(sc, props.toMap, offsetRanges, java.util.Collections.emptyMap[TopicPartition, String]())

  }
}