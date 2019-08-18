package com.atguigu.gmall.canal.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall.canal.util.MyKafkaSender;
import com.atguigu.gmall.common.constant.GmallConstants;

import java.util.List;
import java.util.Random;

public class CanalHandler {

    private String tableName; //表名
    CanalEntry.EventType eventType; //操作类型: insert update delete
    List<CanalEntry.RowData> rowDatasList; //行级数据

    public CanalHandler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowDatasList = rowDatasList;
    }

    /**
     * 将数据批量发送到kafka上
     */
    public void handle() {
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            // 遍历数据集
            for (CanalEntry.RowData rowData : rowDatasList) {
                sendKafka(rowData, GmallConstants.KAFKA_TOPIC_ORDER_INFO);
            }
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                sendKafka(rowData, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
            }
        } else if ("user_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                sendKafka(rowData, GmallConstants.KAFKA_TOPIC_USER_INFO);
            }
        }
    }

    /**
     * 数据发往Kafka
     *
     * @param rowData: 一行数据
     * @param topic:   Kafka主题
     */
    public void sendKafka(CanalEntry.RowData rowData, String topic) {
        JSONObject json = new JSONObject();
        // 将数据封装为json
        for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
            json.put(column.getName(), column.getValue());
        }

        try {
            Thread.sleep(new Random().nextInt(3)*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 发往Kafka
        MyKafkaSender.send(topic, json.toJSONString());
        System.out.println(json.toJSONString());
    }
}
