package com.ibm.mdm.bean;

import com.ibm.mdm.util.DateUtils;

import java.util.Date;

/**
 * Author  Johnny
 * Create  2018-07-16
 * Desc
 */
public class ChangeLog extends BaseBean{

    private static final long serialVersionUID = 1L;

    private String platform;

    private String viewName;

    private String changeDesc;

    private String buildRequest;

    private String relatedStory;

    private Date updateDate;

    private String updateDateStr;

    public String getUpdateDateStr() {
        if(updateDate!=null){
            updateDateStr= DateUtils.getFormatStrFromDate(updateDate,DateUtils.yyyy_MM_dd);
        }
        return updateDateStr;
    }

    public void setUpdateDateStr(String updateDateStr) {
        this.updateDateStr = updateDateStr;
    }

    public String getViewName() {
        return viewName;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    public String getChangeDesc() {
        return changeDesc;
    }

    public void setChangeDesc(String changeDesc) {
        this.changeDesc = changeDesc;
    }

    public String getBuildRequest() {
        return buildRequest;
    }

    public void setBuildRequest(String buildRequest) {
        this.buildRequest = buildRequest;
    }

    public String getRelatedStory() {
        return relatedStory;
    }

    public void setRelatedStory(String relatedStory) {
        this.relatedStory = relatedStory;
    }

    public Date getUpdateDate() {
        return updateDate;
    }

    public void setUpdateDate(Date updateDate) {
        this.updateDate = updateDate;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

}
