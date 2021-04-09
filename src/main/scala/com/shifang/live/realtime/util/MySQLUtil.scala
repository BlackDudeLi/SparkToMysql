package com.shifang.live.realtime.util

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * 读取MySQL配置
 */
object MySQLUtil {
  def mysqlConnection(): Connection = {
    DriverManager.getConnection("jdbc:mysql://bigdata01:3306/shifang_live?characterEncoding=UTF-8", "root", "951128")
  }
}

/**
    建表 live_realtime

  CREATE TABLE `live_realtime` (
    `user_id` int(11) NOT NULL,
    `content_id` int(11) NOT NULL,
    `live_id` int(11) NOT NULL,
    `live_number` varchar(50) NOT NULL,
    `join_time` date DEFAULT NULL
  ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

 */

/**
    建表 live_count

    CREATE TABLE `live_count` (
    `live_id` int(11) NOT NULL,
    `live_number` varchar(50) NOT NULL,
    `content_id` int(11) NOT NULL,
    `user_count` varchar(50) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

 */