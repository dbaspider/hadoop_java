package com.qf.ca.cadp.common.pojo.controller;

//import io.swagger.annotations.ApiModel;
//import io.swagger.annotations.ApiModelProperty;

//@ApiModel(description = "统一响应类型")
public class ResponseHeader {

    //@ApiModelProperty(value = "响应状态", name = "status", example = "SUCC")
    protected String status;

    //@ApiModelProperty(value = "响应状态描述", name = "description", example = "your request has benn processed.")
    protected String description;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}
