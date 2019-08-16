package com.atguigu.gmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON

/**
  * 获取样例类的对象
  */
object GetBeanClass {

    /**
      * 将启动日志转为启动日志样例类
      *
      * @param log : 日志
      * @return
      */
    def getStartupClass(log: String): StartupLog = {
        val startupLog: StartupLog = JSON.parseObject(log, classOf[StartupLog])

        val date: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startupLog.ts))

        startupLog.logDate = date.split(" ")(0)
        startupLog.logHour = date.split(" ")(1)

        startupLog
    }

    /**
      * 将事件日志转为事件日志样例类
      *
      * @param log : 日志
      * @return
      */
    def getEventClass(log: String): StartupLog = {
        getStartupClass(log)
    }

    /**
      * 将订单数据转为样例类
      *
      * @param orderInfoString : 订单信息
      * @return
      */
    def getOrderInfoClass(orderInfoString: String): OrderInfo = {
        val orderInfo: OrderInfo = JSON.parseObject(orderInfoString, classOf[OrderInfo])

        val date: Array[String] = orderInfo.create_time.split(" ")
        orderInfo.create_date = date(0)
        orderInfo.create_hour = date(1).split(":")(0)

        orderInfo
    }
}