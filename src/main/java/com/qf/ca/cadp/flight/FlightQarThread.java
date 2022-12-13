package com.qf.ca.cadp.flight;

import com.qf.ca.cadp.common.pojo.model.PlaneDetail;
import com.qf.ca.cadp.common.utils.BaseUtils;
import com.qf.ca.cadp.common.utils.HBaseUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class FlightQarThread {

    private static final Logger logger = LoggerFactory.getLogger(FlightQarThread.class);

    static String zkUrl = "hadoop01:2181";
    static String zkPort = "2181";
    static String tableName = "qar_success3";
    static String colFamily = "cf1";

    static ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors()*2, 2000L,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    public static void main(String[] args) throws IOException {
        long startTime = 1660693394000L; 	// time = 1660693394000
        //long end   = start + 20 * 1000; // time = 1660701438000
        long endTime = 1660701438000L;
        String planeNumber = "B1870";

        long cycleCount = (endTime - startTime) / 1000 / 1000 + 1;
        Map<Long, List<PlaneDetail>> planeDetailsMap = new HashMap<>();
        CountDownLatch countDownLatch = new CountDownLatch((int) cycleCount);
        logger.info("cycleCount = {}", cycleCount);
        logger.info("queryHbase begin");
        try {
            for (int i = 0; i < cycleCount; i++) {
                long tempStart = startTime + i * 1000*1000;
                long tempEnd = startTime + i* 1000*1000 + 999*1000;
                if(i==cycleCount-1){
                    tempEnd = endTime;
                }
                long finalTempEnd = tempEnd + 1;
                //logger.info("getPlaneDetailMap: {} - {}", tempStart, finalTempEnd);
                poolExecutor.execute(()-> {
                    try {
                        getPlaneDetailMap(planeDetailsMap, planeNumber, tempStart, finalTempEnd, countDownLatch);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }

            countDownLatch.await(3600l, TimeUnit.SECONDS);
            logger.info("resultScannerMap's size is {}", planeDetailsMap.size());
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<List<PlaneDetail>> planeDetailsList = new ArrayList<>();
        planeDetailsMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEachOrdered(x -> planeDetailsList.add(x.getValue()));;
        int rows = 0;
        for (List<PlaneDetail> planeDetailList : planeDetailsList) {
            //planeDetails.addAll(planeDetailList);
            rows += planeDetailList.size();
        }

        logger.info("rows = " + rows);
        logger.info("queryHbase end");
    }

    private static void getPlaneDetailMap(Map <Long, List<PlaneDetail>> planeDetailMap, String planeNumber, long beginTime, long endTime, CountDownLatch countDownLatch) throws IOException {
        logger.info("getPlaneDetailMap: {} - {}", beginTime, endTime);
        byte[] startRow = HBaseUtils.resolutionKey2Bytes(new StringBuilder(planeNumber).reverse().toString(), beginTime);
        byte[] endRow = HBaseUtils.resolutionKey2Bytes(new StringBuilder(planeNumber).reverse().toString(), endTime);
        LinkedList<PlaneDetail> planeDetails = new LinkedList<>();
        int rows = 0;
        try (ResultScanner results = HBaseUtils.multiRowBase(zkUrl, zkPort, tableName, startRow, endRow)) {
            for (Result r : results) {
                // long time = HBaseUtils.bytes2ResolutionKey(r.getRow())._2;
                // String key = HBaseUtils.bytes2ResolutionKey(r.getRow())._1;
                // for (Cell cell : r.rawCells()) {
                //   String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                //   logger.info("time = " + time + " key = " + key + " cf = " + cf);
                // }
                rows++;
                addPlaneDetail(planeDetails, r);
            }
        }
        logger.info("sub rows = {} / {} - {}", rows, beginTime, endTime);
        planeDetailMap.put(beginTime, planeDetails);
        countDownLatch.countDown();
    }

    private static void addPlaneDetail(LinkedList<PlaneDetail> planeDetails, Result r) throws IOException {
        PlaneDetail planeDetail = new PlaneDetail();
        planeDetail.setTime(HBaseUtils.bytes2ResolutionKey(r.getRow())._2);
        // 遍历rowkey对应的所有列
        for (Cell cell : r.rawCells()) {
            if (ArrayUtils.isEmpty(CellUtil.cloneValue(cell))) {
                continue;
            }
            // 采集参数
            if (colFamily.equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                TreeMap treeMap = BaseUtils.json2Obj(HBaseUtils.unCompress(CellUtil.cloneValue(cell)), TreeMap.class);
                HashMap<String, String> stringStringHashMap = new HashMap<>();
                Set set = treeMap.keySet();
                for (Object key : set) {
                    stringStringHashMap.put(key.toString(), treeMap.get(key) == null ? "" : treeMap.get(key).toString());
                }
                planeDetail.setCollectParamResult(stringStringHashMap);
            }
//            // 自定义参数
//            if (qarConfig.getCfCustom().equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
//                TreeMap treeMap = BaseUtils.json2Obj(HBaseUtils.unCompress(CellUtil.cloneValue(cell)), TreeMap.class);
//                HashMap<String, String> stringStringHashMap = new HashMap<>();
//                Set set = treeMap.keySet();
//                for (Object key : set) {
//                    stringStringHashMap.put(key.toString(), treeMap.get(key) == null ? "" : treeMap.get(key).toString());
//                }
//                planeDetail.setCustomParamResult(stringStringHashMap);
//            }
//            // 标准参数
//            if (qarConfig.getCfStandard().equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
//                TreeMap treeMap = BaseUtils.json2Obj(HBaseUtils.unCompress(CellUtil.cloneValue(cell)), TreeMap.class);
//                HashMap<String, String> stringStringHashMap = new HashMap<>();
//                Set set = treeMap.keySet();
//                for (Object key : set) {
//                    stringStringHashMap.put(key.toString(), treeMap.get(key) == null ? "" : treeMap.get(key).toString());
//                }
//                planeDetail.setStandardParamResult(stringStringHashMap);
//            }
        }
        // 添加到返回对象中
        planeDetails.add(planeDetail);
    }
}
