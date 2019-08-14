package com.atguigu.gmall.realtime.bean

/**
  * 事件日志样例类
  *
  * @param area    : 地区
  * @param uid     : 用户id
  * @param itemid  : 商品编号
  * @param npgid   : 跳转页id
  * @param evid    : 事件id
  * @param os      : 操作系统
  * @param pgid    : 当前页id
  * @param appid   : 应用id
  * @param mid     : 设备id
  * @param type    : 日志类型
  * @param logDate : 日期
  * @param logHour : 小时
  * @param ts      : 时间戳
  */
case class EventLog(
                       area: String,
                       uid: String,
                       itemid: String,
                       npgid: String,
                       evid: String,
                       os: String,
                       pgid: String,
                       appid: String,
                       mid: String,
                       `type`: String,
                       var logDate: String,
                       var logHour: String,
                       ts: Long
                   )
