package com.ibm.mdm.bean;

import com.fasterxml.jackson.databind.ser.Serializers;

/**
 * Author  Johnny
 * Create  2018-09-10
 * Desc
 */
public class SampleData extends BaseBean{

    private String head;

    private String cValue;

    private String cType;

    public String getcValue() {
        return cValue;
    }

    public void setcValue(String cValue) {
        this.cValue = cValue;
    }

    public String getcType() {
        return cType;
    }

    public void setcType(String cType) {
        this.cType = cType;
    }

    public String getHead() {
        return head;
    }

    public void setHead(String head) {
        this.head = head;
    }



}
