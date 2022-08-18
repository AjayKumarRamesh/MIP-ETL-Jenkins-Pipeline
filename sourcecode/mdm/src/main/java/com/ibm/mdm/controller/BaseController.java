package com.ibm.mdm.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.bind.annotation.ModelAttribute;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.UnsupportedEncodingException;

/**
 * Author  Johnny
 * Create  2018-06-15
 * Desc
 */
public class BaseController {

    Log logger= LogFactory.getLog(this.getClass());

    protected HttpServletRequest request;
    protected HttpServletResponse response;
    protected HttpSession session;

    @ModelAttribute
    public void setReqAndRes(HttpServletRequest request, HttpServletResponse response){
        this.request = request;
        try {
            this.request.setCharacterEncoding("UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.error("this.request.setCharacterEncoding(\"UTF-8\");", e);
        }

        this.response = response;
        this.response.setContentType("application/x-www-form-urlencoded");
        this.response.setCharacterEncoding("UTF-8");
        this.session = request.getSession();
    }

}
