package com.ibm.mdm.security;

import com.alibaba.fastjson.JSON;
import com.ibm.mdm.util.HttpClientUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Author  Johnny
 * Create  2018-06-15
 * Desc
 */
@Component
public class W3auth {

    Log logger= LogFactory.getLog(this.getClass());

    @Value("${w3auth.clientId}")
    private String clientId;

    @Value("${w3auth.clientSecret}")
    private String clientSecret;

    @Value("${w3auth.introspectUri}")
    private String introspectUri;

    @Value("${w3auth.authUrl}")
    private String authUrl;

    @Value("${w3auth.redirectUri}")
    private String redirectUri;

    @Value("${w3auth.accessTokenUri}")
    private String accessTokenUri;

    public String getAuthorizeURL(HttpServletRequest request){
        String state= UUID.randomUUID().toString();
        request.getSession().setAttribute("state",state);
        String url=this.authUrl;
        String param= "response_type=code" + "&client_id=" + this.clientId
                + "&redirect_uri=" + this.redirectUri+"&scope=openid";
        return url + "?" + param;
    }

    public String getAccessToken(String code){
        String url=this.accessTokenUri;
        Map<String,String> map=new HashMap<String,String>();
        map.put("client_id",clientId);
        map.put("client_secret",clientSecret);
        map.put("grant_type","authorization_code");
        map.put("redirect_uri",this.redirectUri);
        map.put("code",code);
        String result=HttpClientUtil.post(url,map);
        Map mapTypes = JSON.parseObject(result);
        String token=(String)mapTypes.get("access_token");
        return token;
    }

    public String getUserEmail(String token){
        String url=this.introspectUri;
        Map<String,String> map=new HashMap<String,String>();
        map.put("client_id",clientId);
        map.put("client_secret",clientSecret);
        map.put("token",token);
        String result=HttpClientUtil.post(url,map);
        Map mapTypes = JSON.parseObject(result);
        String email=(String)mapTypes.get("sub");
        return email;
    }
}
