package com.shawnks.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/** @auther Xiaozhuang Song.
 * @date 2020/11/23.
 * @time 10:36
 * @project UserBehaviorAnalyse
 *          Copyright(c) Shawn Song All Rights Reserved
 */
//定义输入数据的样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
//define the result class of window aggregation
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object Hotitems {
  def main(args: Array[String]): Unit = {
//  1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setMaxParallelism(8)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//  2.读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")



//    val dataStream = env.readTextFile("F:\\CS\\git\\userBehaviorAnalysis_Flink\\UserBehaviorAnalyse\\HotitemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      val dataStream = env.addSource( new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
      .map(data =>{
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim,
          dataArray(4).trim.toLong)

      }).assignAscendingTimestamps(_.timestamp * 1000L)

    val processedStream = dataStream.filter(_.behavior == "pv").keyBy(_.itemId).timeWindow(Time.hours(1),Time.minutes
    (5)).aggregate(new CountAgg(), new WindowResult())    //窗口聚合
      .keyBy(_.windowEnd)     //按照窗口分组
      .process( new TopNHotItems(3))


//    3.sink:控制台输出
    processedStream.print()

    env.execute("hot items job")

  }
}

//自定义预聚合函数
class CountAgg() extends  AggregateFunction[UserBehavior, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//自定义预聚合函数计算平均数
class AverageAgg() extends  AggregateFunction[UserBehavior, (Long , Int), Double]{
  override def createAccumulator(): (Long, Int) = ( 0L, 0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1 + in.timestamp, acc._2 + 1)

  override def getResult(acc: (Long, Int)): Double = acc._1 / acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1 + acc1._1, acc._2 + acc1._2)
}

//自定义窗口函数计算ItemViewCount
class WindowResult() extends  WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String]{

  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state",classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                              collector: Collector[String]): Unit = {
//    把每条数据存入状态列表
    itemState.add(value)
//    注册一个定时器
    context.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

//定时器触发时，对所有数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
//  将所有state中的数据取出，放到一个Listbuffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for( item <- itemState.get()){
      allItems += item
    }
//    按照count大小排序
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    itemState.clear()

    val result: StringBuilder = new StringBuilder
    result.append("Time is : ").append(new Timestamp( timestamp - 1 )).append("\n")

    for( i <- sortedItems.indices){
      val currentItem = sortedItems(i)
      result.append("No").append(i + 1).append(":").append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append(" \n")
    }
    result.append("==========================")
    Thread.sleep(1000)
    out.collect((result.toString()))

  }
}