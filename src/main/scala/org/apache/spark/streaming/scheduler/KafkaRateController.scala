package org.apache.spark.streaming.scheduler

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobEnd
import java.util.Date
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Duration

class KafkaRateController(val rateEstimator: PIDRateEstimator)
    extends SparkListener {
  var jobUseTime = -1L //实际的逻辑运行时间，但是一个streaming可以有多个job，要累加起来
  var jobStartTime = -1L
  var batchSubmitTime = -1L //batch的启动时间
  var scheduleDelay = -1L
  var rateLimit: Long = 0L
  var currentElems = 0L
  
  def setBatchSubmitTime(batchSubmitTime:Long){
    this.batchSubmitTime=batchSubmitTime
  }
  
  def setCurrentElems(currentElems:Long){
    this.currentElems=currentElems
  }
  override def onJobStart(jobStart: SparkListenerJobStart) {
    jobStartTime = new Date().getTime
    scheduleDelay = jobStartTime - batchSubmitTime
    println("scheduleDelay ", scheduleDelay)
  }
  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    val jobendtime = new Date().getTime
    jobUseTime = jobendtime - jobStartTime
    println("job end ", jobUseTime)
    computeAndPublish(new Date().getTime, currentElems, jobUseTime, scheduleDelay)
  }
  /**
   * Compute the new rate limit and publish it asynchronously.
   */
  private def computeAndPublish(
    time: Long,
    elems: Long,
    workDelay: Long,
    waitDelay: Long) {
    rateEstimator.compute(time, elems, workDelay, waitDelay).foreach { x =>
      rateLimit = x.toLong
    }

  }

  def getLatestRate(): Long = rateLimit

}