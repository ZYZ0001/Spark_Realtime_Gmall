package com.atguigu.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.common.util.MyDateUtil;
import com.atguigu.gmall.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    //访问路径: http://publisher:8070/realtime-total?date=2019-02-01
    //数据示例: [{"id":"dau","name":"新增日活","value":1200}, {"id":"new_mid","name":"新增设备","value":233} ]
    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date) {
        ArrayList<Map> list = new ArrayList<>();

        // 获取新增日活
        Long dauTotal = publisherService.getDauTotal(date);
        Map<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);
        list.add(dauMap);

        // 获取新增设备
        Long newMidTotal = publisherService.getNewMidTotal(date);
        Map<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", newMidTotal);
        list.add(newMidMap);

        return JSON.toJSONString(list);
    }

    //访问路径: http://publisher:8070/realtime-hour?id=dau&date=2019-02-01
    //数据示例: {"yesterday":{"11":383,"12":123,"17":88,"19":200 }, "today":{"12":38,"13":1233,"17":123,"19":688 }}
    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {
        if ("dau".equals(id)) {
            Map<String, Map> map = new HashMap<>();

            // 获取昨天的数据
            Map<String, Long> yesterdayHourDau = publisherService.getHourDauCount(MyDateUtil.getYesterday(date));
            map.put("yesterday", yesterdayHourDau);

            // 获取今天的数据
            Map<String, Long> todayHourDau = publisherService.getHourDauCount(date);
            map.put("today", todayHourDau);

            return JSON.toJSONString(map);

        } else {
            // 其他的需求
            return null;
        }
    }
}
