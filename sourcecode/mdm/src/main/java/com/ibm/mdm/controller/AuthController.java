package com.ibm.mdm.controller;

import com.ibm.mdm.bean.SessionKeys;
import com.ibm.mdm.bean.UserInfo;
import com.ibm.mdm.security.W3auth;
import com.ibm.mdm.service.UserService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Author  Johnny
 * Create  2018-06-04
 * Desc
 */

@Controller
public class AuthController extends BaseController{

    Log logger= LogFactory.getLog(this.getClass());

    @Autowired
    UserService userService;

    @Autowired
    W3auth w3auth;

    @RequestMapping("/w3Login")
    public void w3Login(){
        response.setContentType("text/html;charset=utf-8");
        try {
            response.sendRedirect(w3auth.getAuthorizeURL(request));
        }catch(Exception e){
            e.printStackTrace();;
        }

    }

    @RequestMapping("/w3Callback")
    public String w3CallbackValidate(Model model){
        String code=request.getParameter("code")==null?"":request.getParameter("code").toString();
        if(code==null || "".equals(code)){
            model.addAttribute("errorMessage","login failed:code is not valid");
            return "error";
        }
        String token =w3auth.getAccessToken(code);
        String email=w3auth.getUserEmail(token);
        if(email==null || "".equals(email)){
            model.addAttribute("errorMessage","login failed:Email not valid");
            return "error";
        }
        UserInfo user=new UserInfo();
        user.setUserEmail(email);
        userService.initUser(user);
        request.getSession().setAttribute(SessionKeys.SESSION_USER_ID_KEY,email);
        return "redirect:/index";
    }


}
