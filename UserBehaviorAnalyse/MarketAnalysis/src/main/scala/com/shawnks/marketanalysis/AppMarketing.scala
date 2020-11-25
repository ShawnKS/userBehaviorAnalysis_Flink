package com.shawnks.marketanalysis

import java.lang
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
//import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/** @auther Xiaozhuang Song.
 * @date 2020/11/25.
 * @time 14:57
 * @project UserBehaviorAnalyse
 *          Copyright(c) Shawn Song All Rights Reserved
 */
object AppMarketing {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream = env.addSource( new SimulatedEventSource() )
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map( data => {
        ( "dummy key", 1L)
      })
      .keyBy(_._1) //以渠道和行为类型作为key分组
      .timeWindow( Time.hours(1), Time.seconds(10))
      .aggregate(new CountAgg(), new MarketingCountTotal())
    dataStream.print()
    env.execute("app marketing by channel job")
  }
}

class CountAgg() extends AggregateFunction[(String, Long), Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
 class MarketingCountTotal() extends WindowFunction[Long, MarktingViewCount, String, TimeWindow] {
   override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarktingViewCount]): Unit = {
     val startTs = new Timestamp(window.getStart).toString
     val endTs = new Timestamp(window.getEnd).toString
     val count = input.iterator.next()
     out.collect(MarktingViewCount(startTs, endTs, "app marketing", "total", count))
   }
 }