package com.ibm.mdm.dao;

import com.ibm.mdm.bean.UserInfo;
import org.apache.ibatis.annotations.Mapper;

/**
 * Author  Johnny
 * Create  2019-01-09
 * Desc
 */
@Mapper
public interface UserMapper {

    public void insert(UserInfo user);

    public UserInfo getUserByEmail(String email);

    public void update(UserInfo user);

    public Long getUserNumByDays(int days);

}
