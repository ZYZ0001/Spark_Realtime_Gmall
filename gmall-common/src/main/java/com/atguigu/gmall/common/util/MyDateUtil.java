package com.atguigu.gmall.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.lang.time.DateUtils;

import java.util.Calendar;
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

    /**
     * 依据生日获取年龄
     *
     * @param birthdayString
     * @return
     */
    public static Integer getAge(String birthdayString, String pattern) {
        Date birthday = null;
        int age = 0;
        try {
            birthday = new SimpleDateFormat(pattern).parse(birthdayString);
            Calendar cal = Calendar.getInstance();
            if (cal.before(birthday)) {
                throw new Exception("日期晚于当前日期");
            }
            int yearNow = cal.get(Calendar.YEAR); //当前的年
            int monthNow = cal.get(Calendar.MONTH); //当前的月
            int dayNow = cal.get(Calendar.DAY_OF_MONTH); //当前的日

            cal.setTime(birthday);
            int yearBirthday = cal.get(Calendar.YEAR); //生日的年
            int monthBirthday = cal.get(Calendar.MONTH); //生日的月
            int dayBirthday = cal.get(Calendar.DAY_OF_MONTH); //生日的日

            // 计算整岁
            age = yearNow - yearBirthday;
            // 判断今年是否过了生日
            if (monthNow > monthNow) age--;
            if (monthNow == monthBirthday && dayNow < dayBirthday) age--;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return age;
    }

    public static Integer getAge(String birthdayString) {
        return getAge(birthdayString, "yyyy-MM-dd");
    }
}
