package org.apache.spark.func.tool

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.rdd.RDD
import org.apache.spark.kafka.writer.RDDKafkaWriter

trait KafkaImplicittrait {
  implicit def writeKafka[T](rdd:RDD[T])=new RDDKafkaWriter(rdd)
}