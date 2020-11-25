package com.shawnks.networkflow_analysis

import com.shawnks.networkflow_analysis.UniqueVisitor.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/** @auther Xiaozhuang Song.
 * @date 2020/11/24.
 * @time 17:02
 * @project UserBehaviorAnalyse
 *          Copyright(c) Shawn Song All Rights Reserved
 */
object UvWithBloom {
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
        //      在一个大时间窗口内做聚合
        //      UNIX时间戳是从1970年1月1日起经过的秒数。
        //构造函数需要从1970年1月1日开始经过的*毫*秒数。如果你有一个以秒为单位的数字，当然，你必须将它乘以1000。
      }).assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv" ) //只统计pv操作
      .map( data => ("dummyKey", data.userId) )
      .keyBy(_._1)
      .timeWindow(Time.hours(1) )
//      窗口关闭之前操作/来一个处理一个
//      使用触发器trigger
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())
    dataStream.print()
    env.execute("uv with bloom job")
  }
}

class MyTrigger() extends Trigger[(String,Long), TimeWindow] {
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext)
  : TriggerResult = TriggerResult.FIRE_AND_PURGE
//  每来一条数据，就直接触发窗口处理并清空窗口

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult =
    TriggerResult.CONTINUE

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
}

//定义bloom filter
class Bloom(size: Long) extends  Serializable{
//  位图总大小
  private val cop = if( size > 0) size else 1 << 27
//  定义hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for(i <- 0 until value.length){
    result = result * seed + value.charAt(i)
    }
    result & (cop - 1)
  }
}

class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{
//  定义redis连接
  lazy val jedis = new Jedis("localhost",6379)
  lazy val bloom = new Bloom(1<<29)


  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
//    位图的存储方式,key是windowEnd, value是bitmap
    val storeKey = context.window.getEnd.toString
    var count = 0L
//    把每个窗口的uv count 值也存入redis表,存放内容为(windowEnd -> uvCount),所以要先从redis中读取
    if( jedis.hget("count", storeKey) != null){
      count = jedis.hget("count", storeKey).toLong
    }
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)
//    定义一个标识位,盘点redis位图有没有这一位
    val isExist = jedis.getbit(storeKey, offset)
    if(!isExist){
//      如果不存在,位图对应位置1, count + 1
      jedis.setbit(storeKey , offset , true)
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect(UvCount(storeKey.toLong, count +1 ))
//      过滤uv
    } else{
      out.collect( UvCount(storeKey.toLong, count) )
    }
  }
}

