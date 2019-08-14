package com.atguigu.gmall.realtime.util

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * 消费Kafka的数据
  */
object MyKafkaUtil {

    private val properties: Properties = PropertiesUtil.load("config.properties")
    private val brokerList: String = properties.getProperty("kafka.broker.list")

    val kafkaParam = Map(
        // Kafka集群地址
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
        //Key和Value的解码器
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        // 消费者组
        ConsumerConfig.GROUP_ID_CONFIG -> "gmall_consumer_group",
        // 自动重置为最新的偏移量
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
        // 自动提交
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean)
    )

    // 创建DStream，返回接收到的输入数据
    // LocationStrategies：根据给定的主题和集群地址创建consumer
    // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
    // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
    // ConsumerStrategies.Subscribe：订阅一系列主题
    def getKafkaDStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String,String]] = {
        KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
    }
}
