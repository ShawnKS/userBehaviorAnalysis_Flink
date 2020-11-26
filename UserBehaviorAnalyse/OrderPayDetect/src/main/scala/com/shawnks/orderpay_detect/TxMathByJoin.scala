package com.shawnks.orderpay_detect

import com.shawnks.orderpay_detect.TxMatchEdetect.{TxPayMatch, getClass}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/** @auther Xiaozhuang Song.
 * @date 2020/11/26.
 * @time 17:50
 * @project UserBehaviorAnalyse
 *          Copyright(c) Shawn Song All Rights Reserved
 */
object TxMathByJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    读取订单事件流
    val resource = getClass.getResource("/OrderLog.csv")
    //    val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream = env.socketTextStream("localhost",7777)
      .map(data=>{
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim,dataArray(3).trim.toLong)
      })
      .filter(_.txId!="")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)
    //    支付到账事件流
    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    //    val receiptEventStream = env.readTextFile(receiptResource.getPath)
    val receiptEventStream = env.socketTextStream("localhost",8888)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

      val processedStream = orderEventStream.intervalJoin( receiptEventStream )
        .between(Time.seconds(-5),Time.seconds(5))
        .process(new TxPayMatchByJoin())

    processedStream.print()
    env.execute("tx pay match by join job")
    }
}

class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent,(OrderEvent, ReceiptEvent)]{
  override def processElement(in1: OrderEvent, in2: ReceiptEvent, context: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    collector.collect((in1,in2))
  }
}

