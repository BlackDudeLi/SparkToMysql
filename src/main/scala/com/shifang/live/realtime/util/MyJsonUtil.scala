package com.shifang.live.realtime.util

import com.shifang.live.realtime.bean.LiveInfo
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.ShortTypeHints
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization

/**
 * Json解析
 */
object MyJsonUtil {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JSONParse1").setMaster("local")
    val sc = new SparkContext(conf)

    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val input = sc.textFile("E:\\十方教育\\Realtime_live\\src\\main\\resources\\JsonTest.json")

    // 先输出原格式
    input.collect().foreach(x => print(x + ","))
    // 测试
    val first = input.take(1)(0)
    println(first)
    println(first.getClass)
    // 测试 取第一个key
    val p = parse(first).extract[LiveInfo]
    println(p.user_id)
    println("==========")
    // 取出 user_id
    input.collect().foreach(x => {
      var c = parse(x).extract[LiveInfo]
      println(c.user_id+","+c.content_id+","+c.live_id+","+c.live_number+","+c.join_time)
      // println(c.content_id)
    })
  }

}
