package com.atguigu.gmall.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.lang.time.DateUtils;

import java.util.Date;

/**
 * 自定义的日志工具类
 */
public class MyDateUtil {

    /**
     * 通过今天的日期获取昨天的日期
     *
     * @param todayString: 今天的日期字符串
     * @param pattern: 日期格式
     * @return
     */
    public static String getYesterday(String todayString, String pattern) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

        String yesterdayString = null;

        try {
            Date today = simpleDateFormat.parse(todayString);
            Date yesterday = DateUtils.addDays(today, -1);
            yesterdayString = simpleDateFormat.format(yesterday);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return yesterdayString != null ? yesterdayString : "";
    }

    /**
     * 默认的格式获取昨天的日期
     * @param todayString
     * @return
     */
    public static String getYesterday(String todayString) {
        return getYesterday(todayString, "yyyy-MM-dd");
    }
}
