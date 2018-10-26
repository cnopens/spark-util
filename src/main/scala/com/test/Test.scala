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
import org.apache.kafka.common.serialization.StringSerializer
object Test extends SparkKafkaConfsKey {
  val pa = "/home/zzyc/spark/bostest/kafka-key/"
  val broker = "kafka.bj.baidubce.com:9091"
  val topic = "ea6464f767744f6496c1665102a6984c__zzy"
  val topics = Set(topic)
  val p = new Properties();
  val groupId = "test"
  p.setProperty("bootstrap.servers", broker)
  p.setProperty("key.serializer", classOf[StringSerializer].getName)
  p.setProperty("value.serializer", classOf[StringSerializer].getName)
  p.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  p.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  p.setProperty("security.protocol", "SSL")
  p.setProperty("ssl.truststore.location", ("client.truststore.jks"))
  p.setProperty("ssl.keystore.location", ("client.keystore.jks"))
  p.setProperty("ssl.truststore.password", "kafka")
  p.setProperty("ssl.keystore.password", "jat8tp82")

  p.put("enable.auto.commit", "false"); //自动commit
  p.put("key.deserializer.encoding", "UTF8");
  p.put("value.deserializer.encoding", "UTF8");
  p.setProperty("group.id", groupId)
  
  
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    val offsetRanges: Array[OffsetRange] = Array(OffsetRange.create(topic, 0, 0, 5),
      OffsetRange.create(topic, 1, 0, 5),
      OffsetRange.create(topic, 2, 0, 5),
      OffsetRange.create(topic, 3, 0, 5),
      OffsetRange.create(topic, 4, 0, 5),
      OffsetRange.create(topic, 5, 0, 5))
    val rdd = SparkKafkaManager.createKafkaRDD[String,String](sc, p.toMap, offsetRanges)
    rdd.map(_.value()).collect().foreach(x => println(">>>>>> : "+x))
    //KafkaUtils.createRDD[String,String](sc, kp, offsetRanges, java.util.Collections.emptyMap[TopicPartition, String]())
    //(sc, props.toMap, offsetRanges, java.util.Collections.emptyMap[TopicPartition, String]())

  }
}