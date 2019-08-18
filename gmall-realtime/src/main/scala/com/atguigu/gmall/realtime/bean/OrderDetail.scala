package com.atguigu.gmall.realtime.bean

/**
  * 订单详情表
  *
  * @param id          : 编号
  * @param order_id    : 订单id
  * @param sku_name    : 商品名称
  * @param sku_id      : 商品编号
  * @param order_price : 下单价格
  * @param img_url     : 图片名称
  * @param sku_num     : 购买格式
  */
case class OrderDetail(
                          id: String,
                          order_id: String,
                          sku_name: String,
                          sku_id: String,
                          order_price: String,
                          img_url: String,
                          sku_num: String
                      )