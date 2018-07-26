package com.spark.test

import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD

object SparkKafkaContextTest {
  /**
   * 离线方式 读取kafka数据
   * 测试 SparkKafkaContext类
   */
  def main(args: Array[String]): Unit = {
    val groupId = "dataflow-fg"
    val kp = SparkKafkaContext.getKafkaParam(
      brokers,
      groupId,
      "earliest", // last/consum/custom
      "earliest" //wrong_from
    )
    val topics = Set("MST_422")
    val skc = new SparkKafkaContext(
        kp,
      new SparkConf()
        .setMaster("local")
        .set(SparkKafkaContext.MAX_RATE_PER_PARTITION, "10")
        .setAppName("SparkKafkaContextTest"))
    //val sku=new SparkKafkaUtil(kp)
    //val fromOffset=sku.getConsumerOffset(kp, groupId, topics)
    //val s=fromOffset.map{case(tp,l)=>(s"""${tp.topic},${tp.partition},${l}""")}.mkString("|")
    //println(s)
    //.map{case(tp,l)=>tp->(l-10)}
    //(kp, "dataflow-um_test", topics)

    val kafkadataRdd =skc.kafkaRDD[((String, Int, Long), String)](topics, msgHandle2) //根据配置开始读取
   //skc.kafkaRDD(kp, topics, fromOffset)//指定一个位置开始读取

    //RDD.rddToPairRDDFunctions(kafkadataRdd)
    //kafkadataRdd.reduceByKey(_ + _)
    // kafkadataRdd.map(f)

    kafkadataRdd.foreach(println)
    kafkadataRdd.getRDDOffsets().foreach(println)
    //kafkadataRdd.updateOffsets(kp)

  }
}