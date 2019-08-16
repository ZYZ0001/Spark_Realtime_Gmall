package com.atguigu.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    /**
     * 获取当日交易额
     *
     * @param date
     * @return
     */
    public Double getOrderAmount(String date);

    /**
     * 获取每小时交易额
     *
     * @param date
     * @return
     */
    public List<Map> getOrderHourAmount(String date);
}
