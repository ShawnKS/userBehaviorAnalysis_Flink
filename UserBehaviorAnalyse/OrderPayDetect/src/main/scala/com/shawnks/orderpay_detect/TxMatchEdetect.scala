package com.shawnks.orderpay_detect

import com.shawnks.orderpay_detect.OrderTimeout.getClass
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/** @auther Xiaozhuang Song.
 * @date 2020/11/26.
 * @time 15:38
 * @project UserBehaviorAnalyse
 *          Copyright(c) Shawn Song All Rights Reserved
 */

//定义接受流时间的样例类
case class ReceiptEvent( txId:String, payChannel:String, eventTime:Long)

object TxMatchEdetect {
//  定义侧输出流tag
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedpays")
  val unmathcedReceipts = new OutputTag[ReceiptEvent]("unmatchedreceipts")


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

//    将两条流连接起来，共同处理
    val processedStream = orderEventStream.connect(receiptEventStream)
      .process( new TxPayMatch() )
    processedStream.print("matched")
    processedStream.getSideOutput(unmathcedReceipts).print("unmatched Receipts")
    processedStream.getSideOutput(unmatchedPays).print("unmatched Pays")
    env.execute("tx match job")
  }
  class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
//    定义状态来保存已经到达的订单支付事件和到账事件
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]
    ("pay-state",classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new
        ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))
//订单支付事件数据的处理
    override def processElement1(pay: OrderEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent,
      ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
//      判断有没有对应的到账事件
      val receipt= receiptState.value()
      if(receipt != null){
        collector.collect((pay,receipt))
        receiptState.clear()
      }else{
//        如果还没到，那么把pay存入状态，并且注册一个定时器等待
        payState.update(pay)
        context.timerService().registerEventTimeTimer( pay.eventTime * 1000L + 5000L )
      }
    }

    override def processElement2(in2: ReceiptEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent,
      ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val pay = payState.value()
      if(pay !=null) {
        collector.collect((pay, in2))
        payState.clear()
      }
      else{
        receiptState.update(in2)
        context.timerService().registerEventTimeTimer(in2.eventTime * 1000L +5000L)
      }


    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
//      到时间了，如果还没有受到某个事件，那么输出报警
      if(payState.value() != null){
//        receipt没来,输出pay到侧输出流
        ctx.output(unmatchedPays, payState.value())
      }
      if (receiptState.value() != null){
        ctx.output(unmathcedReceipts, receiptState.value())
      }
      payState.clear()
      receiptState.clear()
    }
  }


}
