package com.atguigu.gmall.realtime.util

import java.io.InputStreamReader
import java.util.Properties

/**
  * 读取配置文件
  */
object PropertiesUtil {
    /*def main(args: Array[String]): Unit = {
        val properties: Properties = PropertiesUtil.load("config.properties")
        println(properties.getProperty("kafka.broker.list"))
    }*/

    def load(propertiesName: String): Properties = {
        val properties: Properties = new Properties()
        properties.load(new InputStreamReader((Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName)), "UTF-8"))
        properties
    }
}
