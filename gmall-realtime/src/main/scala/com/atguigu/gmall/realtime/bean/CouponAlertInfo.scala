package com.atguigu.gmall.realtime.bean

/**
  * 实时预警样例类 -- 优惠券领取实时预警
  *
  * @param mid     : 设备id
  * @param uids    : 用户id集合
  * @param itemIds : 商品id集合
  * @param events  : 发送过的所有行为
  * @param ts      : 时间
  */
case class CouponAlertInfo(mid: String,
                           uids: java.util.HashSet[String],
                           itemIds: java.util.HashSet[String],
                           events: java.util.List[String],
                           ts: Long
                          )
