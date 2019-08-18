package com.atguigu.gmall.realtime.app

import java.util

import com.atguigu.gmall.common.constant.GmallConstants
import com.atguigu.gmall.realtime.bean.{CouponAlertInfo, EventLog, GetBeanClass}
import com.atguigu.gmall.realtime.util.{MyESUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
  * 实时预警
  */
object AlertApp {

    //需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且在登录到领劵过程中没有浏览商品。达到以上要求则产生一条预警日志。
    //同一设备，每分钟只记录一次预警。
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("alert_app").setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(5))

        // TODO 获取数据
        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

        // TODO 转为样例类
        val EventDStream: DStream[EventLog] = inputDStream.map(data => GetBeanClass.getEventClass(data.value()))
        EventDStream.cache() //缓冲数据, 防止多线程异常

        // TODO 创建窗口
        //val winDStream: DStream[StartupLog] = EventDStream.window(Seconds(300), Seconds(60))
        val winDStream: DStream[EventLog] = EventDStream.window(Seconds(30), Seconds(5))

        // TODO 依据mid分组
        val groupByMidDStream: DStream[(String, Iterable[EventLog])] = winDStream.map(event => (event.mid, event)).groupByKey()

        // TODO 分析数据是否满足需求
        val isAlertAndInfoDStream: DStream[(Boolean, CouponAlertInfo)] = groupByMidDStream.map {
            case (mid, eventIter) =>
                // 符合预警的条件: evid==coupon的uid>=3; evid不包含clickItem
                val uidsSet: util.HashSet[String] = new util.HashSet[String]() //用户id的集合
                val itemIdsSet = new util.HashSet[String]() // 优惠券的商品id的集合
                val eventsList = new util.ArrayList[String]() // 该设备在该段时间内的所有行为
                var isAlert = true // 使用需要预警

                breakable {
                    for (event <- eventIter) {
                        eventsList.add(event.evid)
                        if ("coupon".equals(event.evid)) {
                            uidsSet.add(event.uid)
                            itemIdsSet.add(event.itemid)
                        } else if ("cliclItem".equals(event.evid)) {
                            isAlert = false
                            break
                        }
                    }
                }

                // 组成元组 (是否需要预警, 预警的样例类)
                (isAlert && uidsSet.size() >= 3, CouponAlertInfo(mid, uidsSet, itemIdsSet, eventsList, System.currentTimeMillis()))
        }

        //isAlertAndInfoDStream.foreachRDD(_.count())

        // TODO 过滤出需要预警的信息
        val AlertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = isAlertAndInfoDStream.filter(_._1)

        // TODO 每分钟预警一次
        // 增加一个id用于每分钟的去重, 同时用于ES上的主键
        val AlertWithIdDStream: DStream[(String, CouponAlertInfo)] = AlertInfoDStream.map {
            case (flag, alertInfo) =>
                val time = alertInfo.ts / 1000L / 60L
                val id = alertInfo.mid + ":" + time
                (id, alertInfo)
        }

        // TODO 保存在ES上
        AlertWithIdDStream.foreachRDD(rdd => {
            rdd.foreachPartition(alertIter =>
                // 批量保存
                MyESUtil.insertBulk(GmallConstants.ES_INDEX_AlERT, alertIter.toList)
            )
        })

        // TODO 启动
        ssc.start()
        ssc.awaitTermination()
    }
}
