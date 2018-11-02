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
import org.apache.spark.kafka.util.KafkaCluster
import org.apache.spark.core.SparkKafkaContext
object Test22 extends SparkKafkaConfsKey {
  def main(args: Array[String]): Unit = {
    println(">>>>>>>>>>2")
    val brokers = "kafka-1:9092,kafka-3:9092,kafka-3:9092"
    val groupId = "test"
    val topic = "smartadsclicklog"
    val topics = Set("smartadsclicklog")
    val props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("group.id", groupId);
    props.put("enable.auto.commit", "false"); //自动commit
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("key.deserializer.encoding", "UTF8");
    props.put("value.deserializer.encoding", "UTF8");

    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    val skc = new SparkKafkaContext(props.toMap, sc)
    //val kc = new KafkaCluster(props.toMap)
    skc.kc.getConsumerOffet(topics).foreach(println)
    println(">>")
    val ast= skc.kc.getLastestOffset(topics)
    ast.foreach(println)
    println(">>")
    val rdd = skc.createKafkaRDD[String, String](topics,2)
    skc.updateOffset(rdd)
    //rdd.foreach(x => println(x.value()))

    
  }
}