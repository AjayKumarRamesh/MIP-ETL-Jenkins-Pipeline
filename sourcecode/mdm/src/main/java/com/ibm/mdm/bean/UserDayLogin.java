package com.ibm.mdm.bean;

import java.util.Date;

/**
 * Author  Johnny
 * Create  2019-01-09
 * Desc
 */
public class UserDayLogin extends BaseBean {

    private Long loginDay;

    private String userEmail;

    private Date createTs;

    public Long getLoginDay() {
        return loginDay;
    }

    public void setLoginDay(Long loginDay) {
        this.loginDay = loginDay;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public Date getCreateTs() {
        return createTs;
    }

    public void setCreateTs(Date createTs) {
        this.createTs = createTs;
    }


}
