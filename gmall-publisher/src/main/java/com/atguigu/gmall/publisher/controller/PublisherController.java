package com.atguigu.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.common.util.MyDateUtil;
import com.atguigu.gmall.publisher.bean.Option;
import com.atguigu.gmall.publisher.bean.Stat;
import com.atguigu.gmall.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

        // 获取新增交易额
        Double orderTotal = publisherService.getOrderAmount(date);
        HashMap<String, Object> orderMap = new HashMap<>();
        orderMap.put("id", "order_amount");
        orderMap.put("name", "新增交易额");
        orderMap.put("value", orderTotal);
        list.add(orderMap);

        return JSON.toJSONString(list);
    }

    //访问路径: http://publisher:8070/realtime-hour?id=dau&date=2019-02-01
    //访问路径: http://publisher:8070/realtime-hour?id=order_amount&date=2019-02-01
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
        } else if ("order_amount".equals(id)) {
            HashMap<String, Map> map = new HashMap<>();

            // 获取昨天数据
            Map<String, Double> yesterdayHourOrder = publisherService.getHourOrderAmount(MyDateUtil.getYesterday(date));
            map.put("yesterday", yesterdayHourOrder);

            // 获取今天数据
            Map<String, Double> todayHourOrder = publisherService.getHourOrderAmount(date);
            map.put("today", todayHourOrder);

            return JSON.toJSONString(map);
        } else {
            // 其他的需求
            return null;
        }
    }

    //http://localhost:8070/sale_detail?date=2019-04-01&&startpage=1&size=5&keyword=手机小米
    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date, @RequestParam("startpage") int startpage,
                                @RequestParam("size") int size, @RequestParam("keyword") String keyword) {
        // 获取查询数据
        Map<String, Object> saleDetailMap = publisherService.getSaleDetail(date, startpage, size, keyword);
        long total = (long) saleDetailMap.get("total");
        Map ageMap = (Map) saleDetailMap.get("ageMap");
        Map genderMap = (Map) saleDetailMap.get("genderMap");
        List saleList = (List) saleDetailMap.get("detail");
        //System.out.println(saleDetail);

        // 分析年龄占比
        Long age_20Count = 0L;
        Long age20_30Count = 0L;
        Long age30_Count = 0L;
        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            int age = Integer.parseInt((String) entry.getKey());
            Long count = (Long) entry.getValue();
            if (age < 20) {
                age_20Count += count;
            } else if (age < 30) {
                age20_30Count += count;
            } else {
                age30_Count += count;
            }
        }
        Double age_20Ratio = Math.round(age_20Count * 1000D / total) / 10D;
        Double age20_30Ratio = Math.round(age20_30Count * 1000D / total) / 10D;
        Double age30_Ratio = Math.round(age30_Count * 1000D / total) / 10D;

        ArrayList<Option> ageRatioList = new ArrayList<>();
        ageRatioList.add(new Option("20岁以下", age_20Ratio));
        ageRatioList.add(new Option("20岁到30岁", age20_30Ratio));
        ageRatioList.add(new Option("30岁及30岁以上", age30_Ratio));

        //分析性别占比
        long femaleCount = (long) genderMap.get("F");
        long maleCount = (long) genderMap.get("M");
        Double femaleRatio = Math.round(femaleCount * 1000D / total) / 10D;
        Double maleRatio = Math.round(maleCount * 1000D / total) / 10D;

        ArrayList<Option> genderRatioList = new ArrayList<>();
        genderRatioList.add(new Option("男", femaleRatio));
        genderRatioList.add(new Option("女", maleRatio));

        System.out.println(genderRatioList);
        //将饼图放在Stat中
        ArrayList<Stat> statList = new ArrayList<>();
        statList.add(new Stat(ageRatioList, "年龄段占比"));
        statList.add(new Stat(genderRatioList, "性别占比"));
        System.out.println(statList);

        HashMap<String, Object> saleResultMap = new HashMap<>();
        saleResultMap.put("total", total);
        saleResultMap.put("stat", statList);
        saleResultMap.put("detail", saleList);

        System.out.println(JSON.toJSONString(saleResultMap));

        return JSON.toJSONString(saleResultMap);
    }
}
