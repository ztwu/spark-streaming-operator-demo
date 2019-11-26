package com.iflytek.edcc.state

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/6/4
  * Time: 15:59
  * Description
  * 状态流管理
  * 如果要使用mapWithState,就需要设置一个checkpoint目录，开启checkpoint机制。
  * 因为key的state是在内存维护的，如果宕机，则重启之后之前维护的状态就没有了，
  * 所以要长期保存它的话需要启用checkpoint，以便恢复数据。
  *
  *
  *
updateStateByKey底层是将preSateRDD和parentRDD进行co-group，
然后对所有数据都将经过自定义的mapFun函数进行一次计算，即使当前batch只有一条数据也会进行这么复杂的计算，
大大的降低了性能，并且计算时间会随着维护的状态的增加而增加。

mapWithstate底层是创建了一个MapWithStateRDD，存的数据是MapWithStateRDDRecord对象，
一个Partition对应一个MapWithStateRDDRecord对象，该对象记录了对应Partition所有的状态，
每次只会对当前batch有的数据进行跟新，而不会像updateStateByKey一样对所有数据计算。
  */

object DemoMapWithState {

  def main(args:Array[String]):Unit={
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    //需要设置检查点
    ssc.checkpoint("checkpoint/mapWithState-checkpoint")

    val data = ssc.socketTextStream("127.0.0.1",9999)

    //mapWithState为PairDStreamFunctions
    //state保留历史记录上每个key对应的value值
    val mapFunction = (key:String, value:Option[Int], state:State[Int]) => {
      val sum = value.getOrElse(0)+state.getOption().getOrElse(0)
      val output = (key, sum)
      println("当前数据："+value.getOrElse(0))
      println("历史数据："+state.getOption().getOrElse(0))
      state.update(sum)
      output
    }

    data.flatMap(x=>x.split(""))
        .map(x=>{
          (x,1)
        })
        //也是用于全局统计key的状态，但是它如果没有数据输入，便不会返回之前的key的状态，有一点增量的感觉。
        .mapWithState(StateSpec.function(mapFunction))
        .print()

    ssc.start()
    ssc.awaitTermination()

  }

}
