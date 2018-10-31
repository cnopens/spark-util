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
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
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
    val c = new KafkaConsumer[String, String](getLocalKp)
    c.subscribe(topics); //订阅topic
    val consum = getConsumerOffet(c)
    consum.foreach(println)
    val last = getLastOffset(c)
    println(">>>>")
    last.foreach(println)
    val sc = new SparkContext(new SparkConf())
    val offsetRanges = last.map { case (tp, l) => val sl = consum.get(tp).get; OffsetRange.create(topic, tp.partition(), sl, l) }.toArray
    val rdd = SparkKafkaManager.createKafkaRDD[String, String](sc, p.toMap, offsetRanges)
    rdd.map(_.value()).collect.foreach(x => println(">>>>>> : " + x))
    updateOffset(c, last.toMap)
    c.close()
  }
  def sparkTest() {
    val c = new KafkaConsumer[String, String](getLocalKp)
    c.subscribe(topics); //订阅topic

    val sc = new SparkContext(new SparkConf())

    for (i <- 0 to 3) {
      println("### start ###")
      val consum = getConsumerOffet(c)
      val last = getLastOffset(c)
      println(consum.head + "," + last.head)
      val offsetRanges = last.map { case (tp, l) => val sl = consum.get(tp).get; OffsetRange.create(topic, tp.partition(), sl, l) }.toArray

      val rdd = SparkKafkaManager.createKafkaRDD[String, String](sc, p.toMap, offsetRanges)

      rdd.map(_.value()).take(5).foreach(x => println(">>>>>> : " + x))

      updateOffset(c, last.toMap)
      println("### end ###")
      Thread.sleep(3000)
    }
    c.close()
    sc.stop()
  }

  def getConsumerOffet(c: KafkaConsumer[String, String]) = {
    c.poll(0)
    val parts = c.assignment() //获取topic等信息
    parts.map { tp => tp -> c.position(tp) }.toMap
  }
  def updateOffset(c: KafkaConsumer[String, String], offset: Map[TopicPartition, Long]) {
    offset.foreach { case (tp, l) => c.seek(tp, l) } //seek可以理解为更新offset的偏移量，这里的seek相当于指针。
    c.commitSync(offset.map { case (tp, l) => tp -> new OffsetAndMetadata(l) }.asJava)

  }
  def getLastOffset(c: KafkaConsumer[String, String]) = {
    c.poll(0) //拉取数据，延迟为10000.也就是缓存里面的10000前的数据，如果为0的话就全部取
    val parts = c.assignment() //获取topic等信息
    val currentOffset = parts.map { tp => tp -> c.position(tp) }.toMap
    c.pause(parts) //不拉取数据
    c.seekToEnd(parts)
    val re = parts.map { ps => ps -> c.position(ps) }
    currentOffset.foreach { case (tp, l) => c.seek(tp, l) } //seek可以理解为更新offset的偏移量，这里的seek相当于指针。
    re
  }

  def getBeginningOffset(c: KafkaConsumer[String, String]) = {
    c.poll(0) //拉取数据，延迟为10000.也就是缓存里面的10000前的数据，如果为0的话就全部取
    val parts = c.assignment() //获取topic等信息、
    val currentOffset = parts.map { tp => tp -> c.position(tp) }.toMap
    c.pause(parts)
    //以下是表示从哪里开始读取
    c.seekToBeginning(parts); //把offset设置到最开始
    val re = parts.map { ps => ps -> c.position(ps) }
    currentOffset.foreach { case (tp, l) => c.seek(tp, l) } //seek可以理解为更新offset的偏移量，这里的seek相当于指针。
    re
  }

  def getLocalKp() = {
    val p = new Properties();
    val groupId = "test"
    p.setProperty("bootstrap.servers", broker)
    p.setProperty("key.serializer", classOf[StringSerializer].getName)
    p.setProperty("value.serializer", classOf[StringSerializer].getName)
    p.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    p.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    p.setProperty("security.protocol", "SSL")
    p.setProperty("ssl.truststore.location", (pa + "client.truststore.jks"))
    p.setProperty("ssl.keystore.location", (pa + "client.keystore.jks"))
    p.setProperty("ssl.truststore.password", "kafka")
    p.setProperty("ssl.keystore.password", "jat8tp82")

    p.put("enable.auto.commit", "false"); //自动commit
    p.put("key.deserializer.encoding", "UTF8");
    p.put("value.deserializer.encoding", "UTF8");
    p.setProperty("group.id", groupId)
    p
  }

}