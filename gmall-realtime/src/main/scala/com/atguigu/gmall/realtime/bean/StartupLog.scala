package com.atguigu.gmall.realtime.bean

/**
  * 启动日志样例类
  *
  * @param mid     : 设备id
  * @param uid     : 用户id
  * @param appid   : 应用id
  * @param area    : 地区
  * @param os      : 操作系统
  * @param ch      : 软件下载渠道
  * @param type : 日志类型
  * @param vs      : 版本号
  * @param logDate : 日期
  * @param logHour : 小时
  * @param ts      : 时间戳
  */
case class StartupLog(mid: String,
                      uid: String,
                      appid: String,
                      area: String,
                      os: String,
                      ch: String,
                      `type`: String = "startup",
                      vs: String,
                      var logDate: String,
                      var logHour: String,
                      ts: Long
                     )
