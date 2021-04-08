package com.shifang.live.realtime.app

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.shifang.live.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 直播业务
 */
object LiveApp {
  def main(args: Array[String]): Unit = {
    // 定义更新状态方法，参数 values 为当前批次频度， state 为以往批次频度
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    // 执行环境
    val sparkConf: SparkConf = new SparkConf().setAppName("ReceiverWC").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    // 检查点
    ssc.checkpoint("./checkpoint")

    // kafka消费者配置
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("shifang"), MyKafkaUtil.kafkaPara))
    // 将每条消息的 KV 取出
    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())

    // WordCount测试
    val result = valueDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    // .print()
    // 有状态处理
    val stateDStream = result.updateStateByKey[Int](updateFunc)

    // 把DStream保存到MySQL数据库中
    stateDStream.foreachRDD(rdd => {
      //内部函数
      def func(records: Iterator[(String,Int)]) {
        var conn: Connection = null
        var stmt: PreparedStatement = null
        try {
          val url = "jdbc:mysql://bigdata01:3306/shifang_live"
          val user = "root"
          val password = "951128"
          conn = DriverManager.getConnection(url, user, password)
          records.foreach(p => {
            // 注意：更新如果重复，就更新
            // val sql = "insert into wordcount(word,count) values (?,?) ON DUPLICATE KEY UPDATE `count`= ?"
            // val sql = "insert into wordcount(word,count) values ('"+p._1+"'," +p._2+")"
            val sql = "insert into streaming_wc(word,count) values (?,?)"
            // val sql = "insert into streaming_wc(word,count) values (?,?) on duplicate key update count=count" + p._2
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, p._1.trim)
            stmt.setInt(2,p._2.toInt)
            stmt.executeUpdate()
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (stmt != null) {
            stmt.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      }

      val repartitionedRDD = rdd.repartition(3)
      repartitionedRDD.foreachPartition(func)
    })

    stateDStream.print()
    // 任务控制
    ssc.start()
    ssc.awaitTermination()

  }
}
