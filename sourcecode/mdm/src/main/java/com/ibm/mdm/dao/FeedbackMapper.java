package com.ibm.mdm.dao;

import com.ibm.mdm.bean.FeedbackInfo;
import org.apache.ibatis.annotations.Mapper;

/**
 * Author  Johnny
 * Create  2018-07-05
 * Desc
 */
@Mapper
public interface FeedbackMapper {

    public void insert(FeedbackInfo bean);

}
