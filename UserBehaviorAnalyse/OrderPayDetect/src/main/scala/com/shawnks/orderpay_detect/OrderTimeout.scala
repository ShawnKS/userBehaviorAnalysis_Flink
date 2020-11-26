package com.shawnks.orderpay_detect

import java.util

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/** @auther Xiaozhuang Song.
 * @date 2020/11/25.
 * @time 22:59
 * @project UserBehaviorAnalyse
 *          Copyright(c) Shawn Song All Rights Reserved
 */
//定义输入订单事件的样例类
case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long)
//定义输出结果样例类
case class OrderResult( orderId: Long, reusltMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

//    读取订单数据
    val resource = getClass.getResource("/OrderLog.csv")
//    val orderEventStream = env.readTextFile(resource.getPath)
      val orderEventStream = env.socketTextStream("localhost",7777)
      .map(data=>{
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim,dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)
    val orderTimeoutputTag = new OutputTag[OrderResult]("orderTimeout")
    val resultStream = patternStream.select(orderTimeoutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())
    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutputTag).print("timeout")
    env.execute("order timeout job")

  }
}

//自定义超时时间序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout")
  }
}

//自定义正常支付时间序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = map.get("follow").iterator().next().orderId
    OrderResult( payedOrderId, "payed successfully" )
  }
}


