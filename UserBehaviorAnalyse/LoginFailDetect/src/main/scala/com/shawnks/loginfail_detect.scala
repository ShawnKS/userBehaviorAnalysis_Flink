package com.shawnks

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/** @auther Xiaozhuang Song.
 * @date 2020/11/25.
 * @time 21:37
 * @project UserBehaviorAnalyse
 *          Copyright(c) Shawn Song All Rights Reserved
 */
//输入的登陆事件样例类

case class LoginEvent(userId: Long, ip:String, eventType: String, eventTime: Long)

case class Warning( userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object loginfail_detect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data =>{
        val dataArray = data.split(",")
        LoginEvent( dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(t: LoginEvent): Long = t.eventTime * 1000L
      })

    val warningStream = loginEventStream.keyBy(_.userId).process((new LoginWarning(2)))

    warningStream.print()
    env.execute("login fail detect job")
  }
}

class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning]{
//  定义状态，保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]
  ("login-fail-state", classOf[LoginEvent]))
  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {
    if( i.eventType == "fail"){
//      如果是失败，判断之前是否有登陆失败事件
      val iter = loginFailState.get().iterator()
      if(iter.hasNext){
//        如果已经有登录失败事件,就比较事件时间
        val firstFail = iter.next()
        if(i.eventTime < firstFail.eventTime + 2){
//          如果两次间隔小于2秒，输出报警
          collector.collect(Warning(i.userId, firstFail.eventTime, i.eventTime,"login fail in 2 seconds"))
        }
//        更新最近一次的登陆失败时间，保存在状态里
        loginFailState.clear()
        loginFailState.add(i)
      } else{
//        如果是第一次登录失败，直接添加到状态
        loginFailState.add(i)
      }

    } else{
//      如果是成功，清空状态
      loginFailState.clear()
    }


//    val loginFailList = loginFailState.get()
//    //    判断类型是否是fail,只判断类型是fail的事件到状态
//    if( i.eventType == "fail" ){
//      if( ! loginFailList.iterator().hasNext ){
////        如果不是第一次，需要创建定时器
//        context.timerService().registerEventTimeTimer( i.eventTime * 1000L + 2000L)
//      }
//      loginFailState.add( i )
//    } else{
////      如果是成功，清空状态
//      loginFailState.clear()
//    }
  }

//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
//// 触发定时器的时候，根据状态里的失败个数决定是否输出报警
//    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
//    val iter = loginFailState.get().iterator()
//    while(iter.hasNext){
//      allLoginFails += iter.next()
//    }
//
//    if( allLoginFails.length >= maxFailTimes){
//      out.collect(Warning(allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime,
//        "login fail in 2 seconds for" + allLoginFails.length + "times."))
//    }
////清空状态
//    loginFailState.clear()
//  }
}
