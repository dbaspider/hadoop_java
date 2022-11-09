package com.qf.ca.cadp.common.pojo.response;

import com.qf.ca.cadp.common.pojo.model.PlaneDetail;
import lombok.Data;

import java.util.List;

@Data
public class PlaneDetailResponse {
    private String planeNumber;     //飞机编号
    private int size;
    private long actualStartTime;   //结果中的真实开始时间
    private long actualEndTime;     //结果中的真实结束时间
    private List<PlaneDetail> details;      //每一秒的飞机状态
}