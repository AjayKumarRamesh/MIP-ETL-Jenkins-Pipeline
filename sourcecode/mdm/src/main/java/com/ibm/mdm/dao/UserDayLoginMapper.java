package com.ibm.mdm.dao;

import com.ibm.mdm.bean.UserDayLogin;
import org.apache.ibatis.annotations.Mapper;
import java.util.Map;

/**
 * Author  Johnny
 * Create  2019-01-09
 * Desc
 */
@Mapper
public interface UserDayLoginMapper {

    public void insert(UserDayLogin bean);

    public long selectByUserDayLogin(UserDayLogin bean);

    public Map<String,String> selectUserDayLoginByDays(int days);

}
