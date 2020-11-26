package com.shawnks.orderpay_detect

import com.shawnks.orderpay_detect.OrderTimeout.getClass
import com.shawnks.orderpay_detect.OrderTimeoutWithoutCep.orderTimeoutOutputTag
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/** @auther Xiaozhuang Song.
 * @date 2020/11/26.
 * @time 10:42
 * @project UserBehaviorAnalyse
 *          Copyright(c) Shawn Song All Rights Reserved
 */

object OrderTimeoutWithoutCep {
  val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")
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
//    定义process function进行超时检测
//    val timeoutWarningStream = orderEventStream.process(new OrderTimeoutWarning())
    val orderResultStream = orderEventStream.process(new OrderPayMatch())

    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
//    timeoutWarningStream.print()
    env.execute("order timeout without cep job")
  }
}

class  OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]
  ("ispayed-state",classOf[Boolean]))
  lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state",
    classOf[Long]))
  override def processElement(i: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
                              collector: Collector[OrderResult]): Unit = {
    val isPayed = isPayedState.value()
    val timerTs = timerState.value()

//    根据事件的类型进行分类判断，做不同的处理逻辑
    if(i.eventType == "create"){
//  如果是create事件，判断pay是否来过
      if(isPayed){
//        如果已经pay过，匹配成功，输出主流，清空状态
        collector.collect(OrderResult(i.orderId, "payed successfully"))
        context.timerService().deleteEventTimeTimer(timerTs)
        isPayedState.clear()
        timerState.clear()
      }else{
//        如果没有pay过，注册定时器等待pay到来
        val ts = i.eventTime * 1000L + 15 * 60 * 1000L
        context.timerService().registerEventTimeTimer(ts)
        timerState.update(ts)
      }
    }else if(i.eventType == "pay"){
//      如果是pay时间，那么判断是否create过,用timer表示
      if(timerTs > 0){
//        如果有定时器，说明已经有create来过
//        继续判断，是否超过了timeout时间
        if(timerTs > i.eventTime * 1000L ){
          //          如果定时器时间还没到,那么输出成功匹配
          collector.collect(OrderResult(i.orderId, "payed successfully"))
        }else{
          context.output( orderTimeoutOutputTag, OrderResult(i.orderId, "payed but already timeout") )
        }
//        输出结束，清空状态
        context.timerService().deleteEventTimeTimer(timerTs)
        isPayedState.clear()
        timerState.clear()
      }
      else{
//        pay先到,更新状态,注册定时器等待create
        isPayedState.update(true)
        context.timerService().registerEventTimeTimer(i.eventTime * 1000L)
        timerState.update(i.eventTime * 1000L)
      }
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit =
//    根据状态的值，判断哪个数据没来
  {if(isPayedState.value()){
//    如果为true，表示ay先到了，没等到create
    ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey,"already payed but not found create log"))
  } else{
//    表示create到了,没等到pay
    ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey,"order timeout"))
  }
    isPayedState.clear()
    timerState.clear()
    }
}

class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
//  保存pay是否来过的状态
lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]
("ispayed-state",classOf[Boolean]))
  override def processElement(i: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {
    val isPayed = isPayedState.value()

    if( i.eventType == "create" && !isPayed){
      context.timerService().registerEventTimeTimer( i.eventTime * 1000L + 15 * 60 * 1000L)
    } else if( i.eventType == "pay"){
//      如果是pay事件，直接把状态改为true
      isPayedState.update(true)
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext,
                       out: Collector[OrderResult]): Unit = {
//    判断isPayed是否为true
    val isPayed = isPayedState.value()
    if(isPayed){
      out.collect(OrderResult(ctx.getCurrentKey, "order payed successfully"))
    }
    else{
      out.collect(OrderResult(ctx.getCurrentKey, "order timeout"))
    }
//    清空状态
    isPayedState.clear()
  }
}
