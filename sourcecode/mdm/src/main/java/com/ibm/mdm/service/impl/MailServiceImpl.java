//package com.ibm.mdm.service.impl;
//
//import com.ibm.mdm.service.MailService;
//import com.ibm.mdm.util.MailUtil;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.mail.javamail.JavaMailSender;
//import org.springframework.stereotype.Service;
//import org.thymeleaf.TemplateEngine;
//
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * Author  Johnny
// * Create  2018-09-05
// * Desc
// */
//@Service
//public class MailServiceImpl implements MailService{
//    @Autowired
//    private TemplateEngine templateEngine;
//
//    @Autowired
//    private JavaMailSender mailSender;
//
//    @Value("${mail.from}")
//    private String from;
//
//    @Value("${mail.to}")
//    private String to;
//
//    @Value("${mail.template}")
//    private String template;
//
//    @Value("${mail.cc}")
//    private String cc;
//
//    @Override
//    public void sendFeedbackTMail(String type,String user,String message)throws Exception{
//        String ccArray[]=null;
//        if(cc!=null){
//            cc=cc+","+user;
//        }else{
//            cc=user;
//        }
//        ccArray=cc.split(",");
//        String subject="["+type.toUpperCase()+"] CMDP DATA MODELING USER FEEDBACK";
//        Map<String,String> map=new HashMap<String,String>();
//        map.put("type",type);
//        map.put("user",user);
//        map.put("message",message);
//        MailUtil.sendTemplateMail(mailSender,templateEngine,from,to.split(","),ccArray,subject,template,map);
//    }
//}
