package com.atguigu.gmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstants
import com.atguigu.gmall.realtime.bean._
import com.atguigu.gmall.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


object SaleApp {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("sale_app").setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(5))

        // TODO 获取order_info数据流
        val orderInfoInputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_ORDER_INFO, ssc)
        val orderDetailInputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)
        val userInfoInputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_USER_INFO, ssc)

        // TODO 转为样例类, 并修改结构为元组
        // 订单表
        val orderInfoDStream: DStream[(String, OrderInfo)] = orderInfoInputDStream.map(data => {
            // 转为样例类
            val orderInfo: OrderInfo = GetBeanClass.getOrderInfoClass(data.value())
            // 脱敏
            orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) + "****" + orderInfo.consignee_tel.substring(7)
            // 改变格式
            (orderInfo.id, orderInfo)
        })
        //订单详情表
        val orderDetailDStream: DStream[(String, OrderDetail)] = orderDetailInputDStream.map(data => {
            // 转为样例类
            val orderDetail: OrderDetail = GetBeanClass.getOrderDetailClass(data.value())
            // 改变格式
            (orderDetail.order_id, orderDetail)
        })
        //用户表
        val userDetailDStream: DStream[(String, UserInfo)] = userInfoInputDStream.map(data => {
            // 转为样例类
            val userDetail: UserInfo = GetBeanClass.getUserInfoClass(data.value())
            // 改变格式
            (userDetail.id, userDetail)
        })

        // TODO 双流join -- 同时保留两边的数据, 使用fullJoin
        val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

        // TODO 双流join后处理发往Redis
        val saleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(iter => {
            implicit val formats = org.json4s.DefaultFormats // 样例类转为字符串不能使用JSON类
            // 分析: 主表: order_info  从表order_detail
            //  主表中订单号只会有一个, 所有使用string类型存在redis中
            //  从表中订单号会重复, 所有使用set类型存储在redis中
            val jedis = MyRedisUtil.getJedis() // 连接Redis
            val saleDetailList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]() // 保存关联两表后的数据(SaleDetail)

            // 如果orderInfo 不为none
            // 1 如果 从表也不为none 关联从表
            // 2 把自己写入缓存
            // 3 查询缓存
            for ((orderId, (orderInfoOption, orderDetailOption)) <- iter) {
                val orderInfoKey = "order_info:" + orderId //order_info表数据在redis中的key名称
                val orderDetailKey = "order_detail:" + orderId //order_detail表数据在redis中的key名称

                if (orderInfoOption != None) {
                    val orderInfo: OrderInfo = orderInfoOption.get
                    if (orderDetailOption != None) {
                        // 关联表
                        val saleDetail = new SaleDetail(orderInfo, orderDetailOption.get)
                        saleDetailList += saleDetail
                    }

                    // orderInfo写入redis
                    // type: string    key: order_info:[order_id]  value: order_info_json
                    val orderInfoJsonString: String = Serialization.write(orderInfo)
                    jedis.setex(orderInfoKey, 300, orderInfoJsonString) // 设置5分钟过期时间, 避免redis中过量数据

                    // 查询缓存中的orderDetail
                    val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
                    import scala.collection.JavaConversions._ //用于java集合转为scala集合
                    if (orderDetailSet.size() != 0) {
                        for (orderDetailJsonString <- orderDetailSet) {
                            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJsonString, classOf[OrderDetail])
                            val saleDetail = new SaleDetail(orderInfo, orderDetail)
                            saleDetailList += saleDetail
                        }
                    }
                } else if (orderDetailOption != None) {
                    //  如果orderInfo 为none  从表不为 none
                    // 1 把自己写入缓存
                    //  order_detail   type:set  key order_detail:order_id , order_detail_json
                    val orderDetail: OrderDetail = orderDetailOption.get
                    val orderDetailJsonString: String = Serialization.write(orderDetail)
                    jedis.sadd(orderDetailKey, orderDetailJsonString)
                    jedis.expire(orderDetailKey, 300)

                    // 2 查询缓存中的order_info
                    val orderInfoJsonString: String = jedis.get(orderInfoKey)
                    val orderInfo: OrderInfo = JSON.parseObject(orderInfoJsonString, classOf[OrderInfo])
                    val saleDetail = new SaleDetail(orderInfo, orderDetail)
                    saleDetailList += saleDetail
                }
            }
            jedis.close()
            saleDetailList.toIterator
        })
        //saleDetailDStream.print()

        // TODO 采集user_info的数据进入redis中, 用于关联数据
        userDetailDStream.foreachRDD(_.foreachPartition(iter => {
            implicit val formats = org.json4s.DefaultFormats // 样例类转为字符串不能使用JSON类
            val jedis: Jedis = MyRedisUtil.getJedis()
            for ((userId, userInfo) <- iter) {
                val userInfoKey = "user_info:" + userId
                val userInfoJsonString = Serialization.write(userInfo)
                //println(userInfoKey+userInfoJsonString)

                jedis.set(userInfoKey, userInfoJsonString) // user_info表的数据保存为string类型
            }
            jedis.close()
        }))

        // TODO 查询redis中的用户信息, 进行关联数据
        val fullSaleDetailDStream: DStream[SaleDetail] = saleDetailDStream.mapPartitions(iter => {
            val jedis: Jedis = MyRedisUtil.getJedis()
            val saleDetailList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]() //保存数据的集合

            for (saleDetail <- iter) {
                // 获取用户数据
                val userInfoJsonString: String = jedis.get("user_info:" + saleDetail.user_id)
                if (userInfoJsonString != null) {
                    val userInfo: UserInfo = JSON.parseObject(userInfoJsonString, classOf[UserInfo])
                    saleDetail.mergeUserInfo(userInfo)
                }
                saleDetailList += saleDetail // 是否关联上用户, 原始数据都应该保留
            }
            jedis.close()
            saleDetailList.toIterator
        })
        //fullSaleDetailDStream.print()

        // TODO 保存在ES上
        fullSaleDetailDStream.foreachRDD(rdd => {
            val saleDetailList: List[SaleDetail] = rdd.collect.toList
            val saleDetailWithKeyList: List[(String, SaleDetail)] = saleDetailList.map(saleDetail => (saleDetail.order_detail_id, saleDetail))
            MyESUtil.insertBulk(GmallConstants.ES_INDEX_SALA, saleDetailWithKeyList)
            println("保存了" + saleDetailList.size + "条数据")
        })

        // TODO 启动
        ssc.start()
        ssc.awaitTermination()
    }
}
