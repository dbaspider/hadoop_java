package com.qf.ca.cadp.common.conf;

import com.qf.ca.cadp.common.pojo.controller.ResponseHeader;

import java.util.HashMap;
import java.util.Map;

public enum MessageType {
    OPT_SUCCESS(CommonConstants.RESP_SUCC,"操作成功"),NO_RESPONSE(CommonConstants.NO_RESPONSE,"无返回值");

    private String Code;

    private String desc;

    private static Map<String, MessageType> valueMap;

    public String getCode() {
        return Code;
    }

    public void setCode(String code) {
        Code = code;
    }

    public String getDesc() {
        return desc;
    }

    public String toName() {
        return toString();
    }

    MessageType(String code, String desc){
        this.desc = desc;
    }

    public static MessageType fromName(String name){
        if(valueMap == null) {
            valueMap = new HashMap<String, MessageType>();
            for(MessageType jt : MessageType.values()){
                valueMap.put(jt.toName(), jt);
            }
        }
        return valueMap.get(name);
    }

    public static ResponseHeader buildResponseHeader(MessageType messageType) {
        ResponseHeader header = new ResponseHeader();
        header.setStatus(messageType.getCode());
        header.setDescription(messageType.getDesc());
        return header;
    }
}
