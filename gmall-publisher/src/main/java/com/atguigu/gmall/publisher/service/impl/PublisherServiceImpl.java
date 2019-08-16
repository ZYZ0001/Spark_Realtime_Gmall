package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.mapper.DauMapper;
import com.atguigu.gmall.publisher.mapper.OrderMapper;
import com.atguigu.gmall.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    // 本次未实现获取新增设备的功能
    @Override
    public Long getNewMidTotal(String date) {
        return 100L;
    }

    @Override
    public Map<String, Long> getHourDauCount(String date) {
        List<Map> dauHourCount = dauMapper.getDauHourCount(date);

        HashMap<String, Long> hourMap = new HashMap<>();

        for (Map map : dauHourCount) {
            hourMap.put(map.get("LOGHOUR").toString(), Long.parseLong(map.get("CT").toString()));
        }

        return hourMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.getOrderAmount(date);
    }

    @Override
    public Map<String, Double> getHourOrderAmount(String date) {
        List<Map> orderHourSum = orderMapper.getOrderHourAmount(date);
        HashMap<String, Double> hourMap = new HashMap<>();

        for (Map map : orderHourSum) {
            hourMap.put(map.get("CREATE_HOUR").toString(), Double.parseDouble(map.get("SUM_AMOUNT").toString()));
        }
        return hourMap;
    }
}
