package com.atguigu.gmall.realtime.bean

import java.text.SimpleDateFormat
import java.util

import com.atguigu.gmall.common.util.MyDateUtil

/**
  * 商品销售详情表
  *
  * @param order_detail_id : 订单详情表编号
  * @param order_id        : 订单编号
  * @param order_status    : 订单状态
  * @param create_time     : 创建事件
  * @param user_id         : 用户id
  * @param sku_id          : 商品id
  * @param user_gender     : 用户性别
  * @param user_age        : 用户年龄
  * @param user_level      : 用户等级
  * @param sku_price       : 商品价格
  * @param sku_name        : 商品名称
  * @param dt              : 时间
  */
case class SaleDetail(
                         var order_detail_id: String = null,
                         var order_id: String = null,
                         var order_status: String = null,
                         var create_time: String = null,
                         var user_id: String = null,
                         var sku_id: String = null,
                         var user_gender: String = null,
                         var user_age: Int = 0,
                         var user_level: String = null,
                         var sku_price: Double = 0D,
                         var sku_name: String = null,
                         var dt: String = null) {
    def this(orderInfo: OrderInfo, orderDetail: OrderDetail) {
        this
        mergeOrderInfo(orderInfo)
        mergeOrderDetail(orderDetail)

    }

    def mergeOrderInfo(orderInfo: OrderInfo): Unit = {
        if (orderInfo != null) {
            this.order_id = orderInfo.id
            this.order_status = orderInfo.order_status
            this.create_time = orderInfo.create_time
            this.dt = orderInfo.create_date
            this.user_id = orderInfo.user_id
        }
    }


    def mergeOrderDetail(orderDetail: OrderDetail): Unit = {
        if (orderDetail != null) {
            this.order_detail_id = orderDetail.id
            this.sku_id = orderDetail.sku_id
            this.sku_name = orderDetail.sku_name
            this.sku_price = orderDetail.order_price.toDouble


        }
    }

    def mergeUserInfo(userInfo: UserInfo): Unit = {
        if (userInfo != null) {
            this.user_id = userInfo.id

            // 生日转为年龄
            this.user_age = MyDateUtil.getAge(userInfo.birthday)
            this.user_gender = userInfo.gender
            this.user_level = userInfo.user_level

        }
    }


}

