package com.atguigu.gmall.common.constant;

public class GmallConstants {
    // Kafka的启动日志主题
    public static final String KAFKA_TOPIC_STARTUP = "GMALL_STARTUP";
    // Kafka的事件日志主题
    public static final String KAFKA_TOPIC_EVENT = "GMALL_EVENT";
    // Kafka的订单主题
    public static final String KAFKA_TOPIC_ORDER_INFO = "GMALL_ORDER";
    // Kafka的订单详情主题
    public static final String KAFKA_TOPIC_ORDER_DETAIL = "GMALL_ORDER_DETAIL";
    // Kafka的用户详情主题
    public static final String KAFKA_TOPIC_USER_INFO = "GMALL_USER";
    // ES的购物券预警名称
    public static final String ES_INDEX_AlERT = "gmall_coupon_alert";
    // ES的订单灵活查询名称
    public static final String ES_INDEX_SALA = "gmall_sale_detail";
}
