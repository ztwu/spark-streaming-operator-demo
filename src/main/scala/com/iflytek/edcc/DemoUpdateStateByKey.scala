package com.iflytek.edcc

import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

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
  */

object DemoUpdateStateByKey {

  def main(args:Array[String]):Unit={
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    //需要设置检查点
    ssc.checkpoint("checkpoint/UpdateStateByKey-checkpoint")

    val data = ssc.socketTextStream("127.0.0.1",9999)

    //state保留历史记录上每个key对应的value值
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      println("当前数据："+currentCount)
      println("历史数据："+state.getOrElse(0))
      Some(currentCount + previousCount)
    }

    data.flatMap(x=>x.split(""))
        .map(x=>{
          (x,1)
        })
        //统计全局的key的状态，但是就算没有数据输入，他也会在每一个批次的时候返回之前的key的状态。
        .updateStateByKey(updateFunc)
        .print()

    ssc.start()
    ssc.awaitTermination()

  }

}
