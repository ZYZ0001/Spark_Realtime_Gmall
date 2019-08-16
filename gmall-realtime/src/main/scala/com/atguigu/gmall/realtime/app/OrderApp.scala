package com.atguigu.gmall.realtime.app

import com.atguigu.gmall.common.constant.GmallConstants
import com.atguigu.gmall.realtime.bean.{GetBeanClass, OrderInfo}
import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
/**
  * 订单处理类
  */
object OrderApp {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("order_app").setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(5))

        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_ORDER_INFO, ssc)

        // TODO 转为样例类
        // TODO 敏感字段脱敏  电话 收件人 地址...
        val orderDStream: DStream[OrderInfo] = kafkaDStream.map(line => {
            // 获取数据
            val order: String = line.value()
            // 转为样例类
            val orderInfo: OrderInfo = GetBeanClass.getOrderInfoClass(order)
            // 电话号码脱敏
            orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) + "****" + orderInfo.consignee_tel.substring(7)
            orderInfo
        })

        orderDStream.print()

        // TODO 保存到HBase
        orderDStream.foreachRDD(rdd => {
            rdd.saveToPhoenix(
                "gmall_order_info",
                Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
                new Configuration,
                Some("hadoop102,hadoop103,hadoop104:2181")
            )
        })

        // TODO 启动
        ssc.start()
        ssc.awaitTermination()
    }
}
