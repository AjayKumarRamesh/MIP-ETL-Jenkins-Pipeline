package com.ibm.mdm.controller;

import com.ibm.bluepages.BPResults;
import com.ibm.bluepages.BluePages;
import com.ibm.mdm.bean.FeedbackInfo;
import com.ibm.mdm.bean.SessionKeys;
import com.ibm.mdm.bean.TableView;
import com.ibm.mdm.service.FeedbackService;
import com.ibm.mdm.service.MailService;
import com.ibm.mdm.service.MdmService;
import com.ibm.swat.password.ReturnCode;
import com.ibm.swat.password.cwa2;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.thymeleaf.util.StringUtils;

import java.util.*;

/**
 * Author  Johnny
 * Create  2018-05-30
 * Desc
 */
@RestController
public class RestApiController extends BaseController{

    Log logger= LogFactory.getLog(this.getClass());

    @Autowired
    MdmService mdmService;

//    @Autowired
//    MailService mailService;

    @Autowired
    FeedbackService feedbackService;

    @GetMapping("/test")
    public BPResults getTable(String username,String password){
      /*  ReturnCode rc;
        cwa2 cwa = new cwa2("bluepages.ibm.com", "bluegroups.ibm.com");
        rc=cwa.authenticate(username, password, "bluepages.ibm.com");*/
        BPResults person = BluePages.getPersonsByInternet(username);
        Hashtable aa=person.getRow(0);
        Set bb=aa.entrySet();
        for(Iterator iter = bb.iterator(); iter.hasNext();){
           // String str = (String)iter.next();
            System.out.println("key: "+iter.next());
            //System.out.println("value: "+aa.get(str));
        }
        System.out.println(person.getRow(0).get("CNUM"));
        return person;
    }

    @PostMapping("/feedback")
    public String feedback(String score,String feedbackType,String feedbackDetail){
        try {
            String userId = (String) session.getAttribute(SessionKeys.SESSION_USER_ID_KEY);
//            if (StringUtils.isEmpty(userId)) {
//                return "0";
//            }
            if (StringUtils.isEmpty(score) || StringUtils.isEmpty(feedbackType) || StringUtils.isEmpty(feedbackDetail)) {
                return "-1";
            }
            FeedbackInfo info = new FeedbackInfo();
            info.setScore(score);
            info.setFeedbackType(feedbackType);
            info.setFeedbackDetail(feedbackDetail);
            info.setUserEmail(userId);
            info.setCreateDate(new Date(System.currentTimeMillis()));
            feedbackService.insert(info);
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                       try {
                           //mailService.sendFeedbackTMail(feedbackType, userId, feedbackDetail);
                       }catch(Exception e) {
                           e.printStackTrace();
                       }
                }
            });
            thread.run();
            return "1";
        }catch (Exception e){
            e.printStackTrace();
            return "-1";
        }

    }
}
