package com.atguigu.gmall.publisher.service;

import java.util.Map;

public interface PublisherService {

    /**
     * 获取日活总数
     *
     * @param date : 日期
     * @return
     */
    public Long getDauTotal(String date);

    /**
     * 获取新增设备数
     *
     * @param date : 日期
     * @return
     */
    public Long getNewMidTotal(String date);

    /**
     * 获取每小时的新增日活数
     *
     * @param date : 日期
     * @return
     */
    public Map<String, Long> getHourDauCount(String date);

    /**
     * 获取当日销售额
     *
     * @param date : 日期
     * @return
     */
    public Double getOrderAmount(String date);

    /**
     * 获取每小时的销售额
     *
     * @param date : 日期
     * @return
     */
    public Map<String, Double> getHourOrderAmount(String date);
}
