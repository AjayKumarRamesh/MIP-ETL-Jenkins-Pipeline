package com.ibm.mdm.util;

import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import javax.mail.internet.MimeMessage;
import java.util.Iterator;
import java.util.Map;

/**
 * Author  Johnny
 * Create  2018-09-05
 * Desc
 */
public class MailUtil {

    public static void sendTemplateMail(JavaMailSender mailSender, TemplateEngine templateEngine,String from, String []to, String []cc, String subject, String mailTemplate, Map<String,String> mailParam)throws Exception{
        MimeMessage mimeMessage=mailSender.createMimeMessage();
        MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true);
        helper.setFrom(from);
        helper.setTo(to);
        if(cc!=null) {
            helper.setCc(cc);
        }
        helper.setSubject(subject);
        Context context = new Context();
        String htmlContent="";
        if(mailParam!=null){
            Iterator entries = mailParam.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry entry = (Map.Entry) entries.next();
                String key = (String)entry.getKey();
                String value = (String)entry.getValue();
                context.setVariable(key, value);
            }
            htmlContent=templateEngine.process(mailTemplate, context);
        }
        helper.setText(htmlContent,true);
        mailSender.send(mimeMessage);
    }
}
