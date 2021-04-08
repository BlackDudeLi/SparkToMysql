package com.shifang.live.realtime.util

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer


/**
 * 读取kafka的工具类
 */
object MyKafkaUtil {
  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")

  // kafka消费者配置
  val kafkaPara: Map[String, Object] = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
      "bigdata01:9092,bigdata02:9092,bigdata03:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "shifang",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

}
