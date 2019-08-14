package com.atguigu.gmall.canal.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall.canal.util.MyKafkaSender;
import com.atguigu.gmall.common.constant.GmallConstants;

import java.util.List;

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
     * 将对order_info表增加的数据发送到kafka上
     */
    public void handle() {
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            // 遍历数据集
            for (CanalEntry.RowData rowData : rowDatasList) {
                JSONObject json = new JSONObject();
                // 将数据封装为json
                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    json.put(column.getName(), column.getValue());
                }
                // 发往Kafka
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER_INFO, json.toJSONString());
                System.out.println(json.toJSONString());
            }
        }
    }

}
