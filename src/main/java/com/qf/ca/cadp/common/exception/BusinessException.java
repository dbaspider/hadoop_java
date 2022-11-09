package com.qf.ca.cadp.common.exception;

import com.qf.ca.cadp.common.conf.MessageType;

public class BusinessException extends RuntimeException {
    private static final long serialVersionUID = -725206047244694569L;

    public BusinessException(String code) {
        super();
        this.code = code;
    }
    
    public BusinessException(String code, Throwable cause) {
        super(cause);
        this.code = code;
    }

    public BusinessException(String code, String message) {
        super(message);
        this.code = code;
        this.msg = message;
    }

    public BusinessException(String code, String message, Throwable cause) {    	
        super(message, cause);
        this.code = code;
        this.msg = message;
    }

    private String code;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    private MessageType messageType;

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    private String msg;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
