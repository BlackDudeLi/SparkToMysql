package com.shifang.live.realtime.app

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.alibaba.fastjson.JSON
import com.shifang.live.realtime.bean.LiveInfo
import com.shifang.live.realtime.util.{MyKafkaUtil, MySQLUtil}
import com.shifang.live.realtime.util.MySQLUtil.mysqlConnection
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 直播业务
 * 主程序入口
 */
object LiveApp {
  def main(args: Array[String]): Unit = {
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
    // 测试 将每条消息取出
    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())

    /**
     * 解析Json并写入到mysql
     *  1.把kafka拿到的Json通过解析，依次插入到第一个表里
     *  2.计算count，写入到第二个表里
     */

    val resultData = valueDStream.foreachRDD(obj => {
      // 每个分区获取一个连接
      obj.foreachPartition(record => {
        val conn = MySQLUtil.mysqlConnection()
        record.foreach(r => {
          // 1. 将数据插入到MySQL
          saveToMysqlLive_realtime(conn, r)
          // 2.累加数据
          // saveToMysqlLive_count()
        })
        conn.close()
      })
    }
    )

    ssc.start()
    ssc.awaitTermination()
  }
    /**
     * MySQL表：live_realtime,5个字段，原始数据参考 JsonTest.json
     * 用来保存原始字段，相当于一个快照表，存储kafka里的数据
     *
     **/
    def saveToMysqlLive_realtime(conn: Connection, str: String): Unit = {
      val live = JSON.parseObject[LiveInfo](str, classOf[LiveInfo])
      val user_id = live.user_id
      val content_id = live.content_id
      val live_number = live.live_number
      val live_id = live.live_id
      val join_time = live.join_time
      //创建一个变量用来保存sql语句
      val sql = "insert into live_realtime (user_id,content_id,live_id,live_number,join_time) values (?,?,?,?,?)"
      //将一条数据存入到mysql
      val ps: PreparedStatement = conn.prepareStatement(sql)
      ps.setInt(1, user_id)
      ps.setInt(2, content_id)
      ps.setInt(3, live_id)
      ps.setInt(4, live_number)
      ps.setString(5, join_time)
      //提交
      ps.execute()
    }

    /**
     * 创建统计表 live_count,用来统计观看直播在线人数
     * 目前4个字段
     *
     */

    /**
     * def saveToMysqlLive_count(conn:Connection,str:String): Unit = {
     * val live = JSON.parseObject[LiveInfo](str, classOf[LiveInfo])
     * val user_id = live.user_id
     * val content_id = live.content_id
     * val live_number = live.live_number
     * val live_id = live.live_id
     * val join_time = live.join_time
     **
     *val sql = "INSERT INTO live_count (live_id,live_number,content_id,user_count) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE user_count = ?"
     * val ps: PreparedStatement = connection.prepareStatement(sql)
     *ps.setInt(1, live_id)
     *ps.setInt(2, )
     *ps.setInt(3,)
     *ps.setInt(4,)
     * //提交
     *ps.execute()
     * }
     * }
     */


    // 定义更新状态方法，参数 values 为当前批次频度， state 为以往批次频度
    def updateFunc(values: Seq[Int], state: Option[Int]) = {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }



}
