package com.ibm.mdm.service.impl;

import com.ibm.mdm.bean.UserDayLogin;
import com.ibm.mdm.bean.UserInfo;
import com.ibm.mdm.dao.UserDayLoginMapper;
import com.ibm.mdm.dao.UserMapper;
import com.ibm.mdm.service.UserService;
import com.ibm.mdm.util.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.Map;

/**
 * Author  Johnny
 * Create  2019-01-10
 * Desc
 */
@Service
public class UserServiceImpl implements UserService {

    @Autowired
    UserMapper userMapper;

    @Autowired
    UserDayLoginMapper userDayLoginMapper;

    @Override
    @Transactional
    public void initUser(UserInfo bean) {
        String email=bean.getUserEmail();
        Date currentDate=new Date(System.currentTimeMillis());
        UserInfo user=userMapper.getUserByEmail(email);
        if(user==null){
            //insert user
            bean.setLastLoginDate(currentDate);
            userMapper.insert(bean);
        }else{
            //update user
            user.setLastLoginDate(currentDate);
            userMapper.update(user);
        }
        String loginDayString= DateUtils.getFormatStrFromDate(currentDate,"yyyyMMdd");
        long loginDay=Long.valueOf(loginDayString);
        UserDayLogin userDayLogin=new UserDayLogin();
        userDayLogin.setLoginDay(loginDay);
        userDayLogin.setUserEmail(email);
        if(userDayLoginMapper.selectByUserDayLogin(userDayLogin)==0){
            userDayLoginMapper.insert(userDayLogin);
        }
    }

    @Override
    public Map<String, String> getLoginUserByDays(int days) {
        return userDayLoginMapper.selectUserDayLoginByDays(days);
    }
}
