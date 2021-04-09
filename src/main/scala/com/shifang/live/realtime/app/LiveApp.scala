package com.shifang.live.realtime.app

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.alibaba.fastjson.JSON
import com.shifang.live.realtime.bean.LiveInfo
import com.shifang.live.realtime.util.MyKafkaUtil
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
     *  解析Json并写入到mysql
     *  这里不会···，
     *  1.怎么把kafka拿到的Json通过解析，依次插入到第一个表里
     *  2.怎么计算count，写入到第二个表里
     *
     */

    val resultData = valueDStream.foreachRDD(_.map(obj => {
      val live = JSON.parseObject[LiveInfo](obj, classOf[LiveInfo])
      live.user_id
      live.content_id
      live.live_number     // 卡在这里·
    })
      resultData.foreach(println)

    )
    // 待解决
    valueDStream.foreachRDD(_.foreach(row => {
      //获取一行转化成list
      val list: List[String] = row.value().split("\t").toList

      saveToMysqlLive_realtime(list, "live_realtime")

      saveToMysqlLive_count(list, "live_count")

    })
    )


//    // 把DStream保存到MySQL数据库中
//    stateDStream.foreachRDD(rdd => {
//      //内部函数
//      def func(records: Iterator[(String,Int)]) {
//        var conn: Connection = null
//        var stmt: PreparedStatement = null
//        try {
//          val url = "jdbc:mysql://bigdata01:3306/shifang_live"
//          val user = "root"
//          val password = "951128"
//          conn = DriverManager.getConnection(url, user, password)
//          records.foreach(p => {
//            // 注意：更新如果重复，就更新
//            // val sql = "insert into wordcount(word,count) values (?,?) ON DUPLICATE KEY UPDATE `count`= ?"
//            // val sql = "insert into wordcount(word,count) values ('"+p._1+"'," +p._2+")"
//            val sql = "insert into streaming_wc(word,count) values (?,?)"
//            // val sql = "insert into streaming_wc(word,count) values (?,?) on duplicate key update count=count" + p._2
//            stmt = conn.prepareStatement(sql);
//            stmt.setString(1, p._1.trim)
//            stmt.setInt(2,p._2.toInt)
//            stmt.executeUpdate()
//          })
//        } catch {
//          case e: Exception => e.printStackTrace()
//        } finally {
//          if (stmt != null) {
//            stmt.close()
//          }
//          if (conn != null) {
//            conn.close()
//          }
//        }
//      }
//
//      val repartitionedRDD = rdd.repartition(3)
//      repartitionedRDD.foreachPartition(func)
//    })
//
//    stateDStream.print()


    ssc.start()
    ssc.awaitTermination()


    /**
     * MySQL表：live_realtime,5个字段，原始数据参考 JsonTest.json
     * 用来保存原始字段，相当于一个快照表，存储kafka里的数据
     *
     * */

    def saveToMysqlLive_realtime(...): Unit = {
      //获取连接
      val connection: Connection = mysqlConnection()
      //创建一个变量用来保存sql语句
      val sql = s"insert into ${tableName} (user_id,content_id,live_id,live_number,join_time) values (?,?,?,?,?,?,?,?,?,?,?)"
      //将一条数据存入到mysql
      val ps: PreparedStatement = connection.prepareStatement(sql)
      ps.setInt(1, data.head)
      ps.setInt(2, data(1))
      ps.setInt(3, data(2))
      ps.setInt(4, data(3))
      ps.setString(5, data(4))
      //提交
      ps.execute()
      connection.close()
    }

    /**
     * 创建统计表 live_count,用来统计观看直播在线人数
     * 目前4个字段
     * 计算逻辑待解决······
     */

    def saveToMysqlLive_count(...): Unit = {
        //获取连接
        val connection: Connection = mysqlConnection()
        //创建一个变量用来保存sql语句
        val sql = "INSERT INTO live_count (live_id,live_number,content_id,user_count) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE user_count = ?"
        //将一条数据存入到mysql
        val ps: PreparedStatement = connection.prepareStatement(sql)
        ps.setInt(1, )
        ps.setInt(2, )
        ps.setInt(3,)
        ps.setInt(4,)
        //提交
        ps.execute()
        connection.close()
       }
    }



    // 定义更新状态方法，参数 values 为当前批次频度， state 为以往批次频度
    def updateFunc (values: Seq[Int], state: Option[Int]) = {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

}
