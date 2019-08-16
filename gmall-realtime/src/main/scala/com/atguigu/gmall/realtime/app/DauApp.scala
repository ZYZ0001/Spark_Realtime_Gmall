package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.atguigu.gmall.common.constant.GmallConstants
import com.atguigu.gmall.realtime.bean.{GetBeanClass, StartupLog}
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

/**
  * 日活处理类
  */
object DauApp {

    def main(args: Array[String]): Unit = {
        // TODO 获取SparkStreaming上下文对象
        val conf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        // TODO 读取kafka数据
        val kafkaDSteam: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

        // TODO 转换为样例类
        val startupClassDStream: DStream[StartupLog] = kafkaDSteam.map(data => {
            val log: String = data.value()
            GetBeanClass.getStartupClass(log)
        })

        // TODO 获取Redis配置信息
        val properties: Properties = PropertiesUtil.load("config.properties")
        val redisHost: String = properties.getProperty("redis.host")
        val redisPort: Int = properties.getProperty("redis.port").toInt

        // TODO 去重
        val distinctDStream: DStream[StartupLog] = startupClassDStream.transform(rdd => {
            //println(s"过滤前: ${rdd.count()}")
            // 批次内去重
            val disRDD: RDD[StartupLog] = rdd.groupBy(_.mid).mapValues(_.take(1)).map(_._2).flatMap(s => s)
            // 获取日活的清单
            val jedis = new Jedis(redisHost, redisPort)
            val dauSet: util.Set[String] = jedis.smembers("dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
            // 广播变量
            val dauBro: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
            // 过滤去重
            disRDD.filter(log => !dauBro.value.contains(log.mid))
            //println(s"过滤后: ${disRDD.filter(log => !dauBro.value.contains(log.mid))}")
        })

        distinctDStream.print()

        // TODO 更新到Redis
        distinctDStream.foreachRDD(rdd => {
            rdd.foreachPartition(logs => {
                // 连接Redis服务
                val jedis = new Jedis(redisHost, redisPort)
                logs.foreach(log => {
                    // 发送set类型的k-v
                    val dauKey: String = "dau:" + log.logDate
                    jedis.sadd(dauKey, log.mid)
                })
                // 关闭
                jedis.close()
            })
        })

        // TODO 发送到HBase
        distinctDStream.foreachRDD(rdd => {
            rdd.saveToPhoenix(
                "gmall_dau",
                Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
                new Configuration,
                Some("hadoop102,hadoop103,hadoop104:2181")
            )
        })

        // TODO 启动
        ssc.start()
        ssc.awaitTermination()
    }
}
