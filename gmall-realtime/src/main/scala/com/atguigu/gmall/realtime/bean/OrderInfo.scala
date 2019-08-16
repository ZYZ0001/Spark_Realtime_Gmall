package com.atguigu.gmall.realtime.bean

/**
  * 订单详情样例类
  *
  * @param id               : 订单编号
  * @param province_id      : 地区编号
  * @param consignee        : 收货人
  * @param order_comment    : 订单备注
  * @param consignee_tel    : 收货人电话
  * @param order_status     : 订单状态
  * @param payment_way      : 付款方式
  * @param user_id          : 用户id
  * @param img_url          : 图片路径
  * @param total_amount     : 总金额
  * @param expire_time      : 失效时间
  * @param delivery_address : 送货地址
  * @param create_time      : 创建时间
  * @param operate_time     : 操作时间
  * @param tracking_no      : 物流单编号
  * @param parent_order_id  : 父订单编号
  * @param out_trade_no     : 订单交易编号(第三方支付用)
  * @param trade_body       : 订单描述(第三方支付用)
  * @param create_date      : 创建日期
  * @param create_hour      : 创建的小时
  */
case class OrderInfo(
                        id: String,
                        province_id: String,
                        consignee: String,
                        order_comment: String,
                        var consignee_tel: String,
                        order_status: String,
                        payment_way: String,
                        user_id: String,
                        img_url: String,
                        total_amount: Double,
                        expire_time: String,
                        delivery_address: String,
                        create_time: String,
                        operate_time: String,
                        tracking_no: String,
                        parent_order_id: String,
                        out_trade_no: String,
                        trade_body: String,
                        var create_date: String,
                        var create_hour: String
                    )
