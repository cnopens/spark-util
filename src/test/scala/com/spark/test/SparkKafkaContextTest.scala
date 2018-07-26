package com.spark.test

import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD

object SparkKafkaContextTest {
  def main(args: Array[String]): Unit = {
    val groupId = "dataflow-fg"
    val kp = SparkKafkaContext.getKafkaParam(
      brokers,
      groupId,
      "earliest", // last/consum/custom/earliest
      "earliest" //wrong_from
    )
    val topics = Set("MST_422")
    val skc = new SparkKafkaContext(
      kp,
      new SparkConf()
        .setMaster("local")
        .set(SparkKafkaContext.MAX_RATE_PER_PARTITION, "10")
        .setAppName("SparkKafkaContextTest"))
    val kafkadataRdd = skc.kafkaRDD[((String, Int, Long), String)](topics, msgHandle2) //根据配置开始读取
    //RDD.rddToPairRDDFunctions(kafkadataRdd)
    kafkadataRdd.foreach(println)
    kafkadataRdd.getRDDOffsets().foreach(println)
    //kafkadataRdd.updateOffsets(kp)

  }
}