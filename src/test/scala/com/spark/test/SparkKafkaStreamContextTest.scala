package com.spark.test

import scala.tools.scalap.Main
import org.apache.streaming.core.SparkKafkaStreamContext
import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import java.util.Date
import kafka.utils.VerifiableProperties
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkKafkaStreamContextTest {
  Logger.getLogger("kafka.utils.VerifiableProperties").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {

    val groupId = "test"
    val kp = SparkKafkaContext.getKafkaParam(
      brokers,
      groupId,
      "last", //last/consum/custom
      "earliest" //wrong_from
    )

    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    sc.setLogLevel("ERROR")
    val skc = new SparkKafkaContext(kp, sc)
    val sksc = new SparkKafkaStreamContext(skc, Seconds(10))
    val topics = Set("smartadsdeliverylog")
    val kstream = sksc.createKafkaStreamRDD(kp, topics)
    kstream.startAndWait { rdd =>
      val start = new Date().getTime
      println(">>>> ", rdd.count)
      val endTime = new Date().getTime
      println("Time : ", endTime - start)
    }
  }
}