package com.atguigu.gmall.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * 监听canal数据, 对数据进行处理
 */
public class CanalClient {

    public static void main(String[] args){
        // 创建连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example", "", ""
        );

        while (true) {
            canalConnector.connect(); //尝试连接
            canalConnector.subscribe("gmall_realtime.*"); //过滤数据
            Message message = canalConnector.get(100); //抓取数据 -- 每次抓取100个message

            // 判断是否抓取到数据
            if (message.getEntries().size() == 0) {
                System.out.println("本次没有抓取到数据...");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    // 仅对行变化的数据进行处理
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        // 将entry中的数据进行反序列化
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                        // 获取一行数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        // 获取对数据操作的类型 insert update delete...
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // 获取操作的表名
                        String tableName = entry.getHeader().getTableName();

                        // 处理数据
                        CanalHandler canalHandler = new CanalHandler(tableName, eventType, rowDatasList);
                        canalHandler.handle();
                    }
                }
            }
        }
    }
}
