package com.spark.test

import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.streaming.core.StreamingDynamicContext
import kafka.serializer.StringDecoder
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka.KafkaDataRDD
import org.slf4j.LoggerFactory

object StreamingDynamicContextTest {
	def msgHandle = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message)
  def main(args: Array[String]): Unit = {

    val brokers = "kafka-2:9092,kafka-1:9092,kafka-3:9092"
    val groupId = "test"
    val kp = SparkKafkaContext.getKafkaParam(
      brokers,
      groupId,
      "consum", // last/consum/custom/earliest
      "last" //wrong_from
      )
    val topics = Set("smartadsdeliverylog")
    val skc = new SparkKafkaContext(
      kp,
      new SparkConf()
        .setMaster("local")
        .set(SparkKafkaContext.MAX_RATE_PER_PARTITION, "10")
        .setAppName("SparkKafkaContextTest"))
    val sskc = new StreamingDynamicContext(skc, Seconds(100))

    val kafkastream = sskc.createKafkaDstream[String, String, StringDecoder, StringDecoder, (String, String)](topics, msgHandle)

    kafkastream.foreachRDD {
      case (rdd) =>
        println("################ start ##################")
        rdd
          .map(x => x._2)
          .collect()
          .foreach { println }
        println("################ END ##################")
        false           //是否马上执行下个批次。否则就等到下一批次时间到来
      // rdd.count > 0 //是否马上执行下个批次。否则就等到下一批次时间到来
    }
    sskc.start()
  }
}