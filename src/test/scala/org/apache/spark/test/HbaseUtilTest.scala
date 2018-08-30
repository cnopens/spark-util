package org.apache.spark.test

import org.apache.spark.hbase.core.SparkHBaseContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client.Scan
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

object HbaseUtilTest {
  val tablename = "test"
  val zk = "zk1,zk2,zk3"
  def f(r: (ImmutableBytesWritable, Result)) = {
    val re = r._2
    new String(re.getRow)
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("tets")
    val sc = new SparkContext(conf)
    val hc = new SparkHBaseContext(sc, zk)
    hc.bulkAllRDD(tablename, f).foreach { println }
    hc.bulkScanRDD(tablename, new Scan(), f)
  }
  def testBulkGet(hc: SparkHBaseContext, rdd: RDD[String]) = {
    hc.bulkGetRDD(tablename, rdd, makeGet, convertResult)
  }
}