package com.qf.ca.cadp.common.pojo.model;

import lombok.Data;

import java.util.Map;

@Data
public class PlaneDetail {
    private long time;      //当前时间
    private Map<String, String> collectParamResult;     //采集参数结果
    private Map<String, String> customParamResult;      //自定义参数结果
    private Map<String, String> smoothParamResult;      //平滑结果

}