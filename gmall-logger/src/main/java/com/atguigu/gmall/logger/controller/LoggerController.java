package com.atguigu.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.common.constant.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class LoggerController {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("log")
    public String dolog(@RequestParam("logString") String logString) {

        // TODO 1.补充时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());

        // TODO 2.落盘
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);

        // TODO 3.推送到kafka
        if ("startup".equals(jsonObject.getString("type"))) {
            // 启动日志
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP, jsonString);
        } else if ("event".equals(jsonObject.getString("type"))) {
            // 事件日志
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, jsonString);
        }

        return "success";
    }
}
