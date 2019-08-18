package com.atguigu.gmall.realtime.util

import java.util.Properties

import redis.clients.jedis.Jedis

object MyRedisUtil {

    def getJedis(): Jedis = {
        val properties: Properties = PropertiesUtil.load("config.properties")
        val host: String = properties.getProperty("redis.host")
        val port: Int = properties.getProperty("redis.port").toInt

        new Jedis(host, port)
    }
}
