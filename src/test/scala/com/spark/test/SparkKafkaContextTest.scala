package com.spark.test

import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
object SparkKafkaContextTest {
  /**
   * 离线方式 读取kafka数据
   * 测试 SparkKafkaContext类
   */
  def main(args: Array[String]): Unit = {
    val brokers="kylin-node2:9092,kylin-node3:9092,kylin-node4:9092"
    val groupId = "test"
     val topics = Set("topic_3")
    val kp = SparkKafkaContext.getKafkaParam(
      brokers,
      groupId,
      "earliest", // last/consum/custom/earliest
      "earliest" //wrong_from
    )
        //如果需要使用ssl验证，需要设置一下四个参数
    kp.put(SparkKafkaContext.DRIVER_SSL_TRUSTSTORE_LOCATION, "/mnt/kafka-key/client.truststore.jks")
    kp.put(SparkKafkaContext.DRIVER_SSL_KEYSTORE_LOCATION, "/mnt/kafka-key/client.keystore.jks")
    kp.put(SparkKafkaContext.EXECUTOR_SSL_TRUSTSTORE_LOCATION, "client.truststore.jks")
    kp.put(SparkKafkaContext.EXECUTOR_SSL_KEYSTORE_LOCATION, "client.keystore.jks")

    val skc = new SparkKafkaContext(kp.toMap, new SparkConf()
      .setMaster("local")
      .set(SparkKafkaContext.MAX_RATE_PER_PARTITION, "1")
      .setAppName("SparkKafkaContextTest"))
   
    val kafkadataRdd = skc.kafkaRDD[String, String](topics)
    kafkadataRdd.foreach(println)

    //RDD.rddToPairRDDFunctions(kafkadataRdd)
    //kafkadataRdd.reduceByKey(_+_)
    // kafkadataRdd.map(f)

    //kafkadataRdd.updateOffsets(kp)

  }
}