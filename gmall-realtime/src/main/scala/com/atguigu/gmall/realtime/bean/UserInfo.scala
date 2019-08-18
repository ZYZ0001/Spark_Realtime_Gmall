package com.atguigu.gmall.realtime.bean

/**
  * 用户详情表
  *
  * @param id         : 编号
  * @param login_name : 用户名称
  * @param user_level : 用户级别
  * @param birthday   : 用户生日
  * @param gender     : 用户性别
  */
case class UserInfo(id: String,
                    login_name: String,
                    user_level: String,
                    birthday: String,
                    gender: String)

