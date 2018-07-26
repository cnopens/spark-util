package com.spark.test

import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.kafka.util.SparkKafkaUtil

object SparkKafkaUtilTest {
  def main(args: Array[String]): Unit = {
    val broker = "kafka03:9092,kafka02:9092,kafka01:9092"
    val groupid = "dataflow-fg"
    //val topics = "MST_417".split(",").toSet
    val topics = "MST_422".split(",").toSet
    val kp = SparkKafkaContext.getKafkaParam(
      broker,
      groupid,
      "consum", // last/consum
      "last" //wrong_from
    )
    val sku = new SparkKafkaUtil(kp)
    //sku.updataOffsetToEarliest(topics, kp)
    //sku.getConsumerOffset(kp, groupid, topics).foreach(println)
    //sku.updataOffsetToLastest(topics, kp)
    //sku.getEarliestOffsets(topics, kp)
    val cu=sku.getConsumerOffset(groupid, topics)
    val la=sku.getLatestOffsets(topics)
    la
    .toList
    .map{case(tp,l)=>
      println(tp,l-cu(tp))
      (tp.topic,l-cu(tp))}
    .groupBy(_._1)
    .foreach{case(t,m)=>println(t,m.map(_._2).sum)}
    //最早的
    //val offsets=s"""mac_probelog,0,480645996|mac_probelog_wifi,1,16622577|mac_probelog_wifi,4,16261842|mac_probelog_wifi,3,16036652|mac_probelog_wifi,5,17184533|mac_probelog_wifi,0,16416957|mac_probelog_wifi,2,15432487"""
    //sku.updataOffsetToCustom(kp, offsets)
  }
}
