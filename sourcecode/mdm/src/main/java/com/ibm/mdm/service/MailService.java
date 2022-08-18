package com.ibm.mdm.service;

/**
 * Author  Johnny
 * Create  2018-09-05
 * Desc
 */
public interface MailService {

    public void sendFeedbackTMail(String type,String user,String message)throws Exception;

}
