package com.atguigu.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    /**
     * 获取日活总数
     *
     * @param date
     * @return
     */
    public Long getDauTotal(String date);

    /**
     * 获取每小时新增日活
     *
     * @param date
     * @return
     */
    public List<Map> getDauHourCount(String date);
}
