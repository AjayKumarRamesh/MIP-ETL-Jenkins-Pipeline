package com.ibm.mdm.service.impl;

import com.ibm.mdm.bean.FeedbackInfo;
import com.ibm.mdm.dao.FeedbackMapper;
import com.ibm.mdm.service.FeedbackService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Author  Johnny
 * Create  2018-07-05
 * Desc
 */

@Service
public class FeedbackServiceImpl implements FeedbackService{

    @Autowired
    FeedbackMapper feedbackMapper;

    @Override
    public void insert(FeedbackInfo bean) {
        feedbackMapper.insert(bean);
    }
}
