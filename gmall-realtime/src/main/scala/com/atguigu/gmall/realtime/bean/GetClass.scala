package com.atguigu.gmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON

/**
  * 获取样例类的对象
  */
object GetClass {

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
}