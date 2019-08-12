package com.atguigu.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class LoggerController {

    @Autowired
//    KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("log")
    public String dolog(@RequestParam("logString") String logString) {

        // 补充时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());

        // 落盘
        log.info(jsonObject.toJSONString());

        // 推送到kafka


        return "success";
    }
}
