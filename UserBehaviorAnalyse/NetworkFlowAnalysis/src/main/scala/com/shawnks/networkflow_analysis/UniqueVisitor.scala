package com.shawnks.networkflow_analysis

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/** @auther Xiaozhuang Song.
 * @date 2020/11/24.
 * @time 14:57
 * @project UserBehaviorAnalyse
 *          Copyright(c) Shawn Song All Rights Reserved
 */

//case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class UvCount( windowEnd: Long, uvCount: Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data =>{
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim,
          dataArray(4).trim.toLong)

      }).assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv" ) //只统计pv操作
      .timeWindowAll(Time.hours(1))
//      在一个大时间窗口内做聚合
      .apply( new UvCountByWindow() )

    dataStream.print()
    env.execute("uv job")
  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
//    定义一个scala set,用于保存所有的数据userId并去重
    var idSet = Set[Long]()
//    把当前窗口所有数据的ID收集到set种，最后输出set的大小
    for( userBehaviour <- input){
      idSet += userBehaviour.userId
    }
    out.collect(UvCount(window.getEnd, idSet.size))
  }
}
