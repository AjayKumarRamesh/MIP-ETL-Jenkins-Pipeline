package com.ibm.mdm.service;

import com.ibm.mdm.bean.UserInfo;

import java.util.Map;

/**
 * Author  Johnny
 * Create  2019-01-10
 * Desc
 */
public interface UserService {

    public void initUser(UserInfo bean);

    public Map<String,String> getLoginUserByDays(int days);
}
