package com.qf.ca.cadp.flight;

import com.alibaba.fastjson.JSON;
import com.qf.ca.cadp.common.pojo.model.PlaneDetail;
import com.qf.ca.cadp.common.pojo.response.PlaneDetailResponse;
import com.qf.ca.cadp.common.utils.BaseUtils;
import com.qf.ca.cadp.common.utils.HBaseUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.util.RamUsageEstimator;
import org.example.utils.FileUtil;
import org.github.jamm.MemoryMeter;
import org.openjdk.jol.vm.VM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class FlightAirFase {

    private static final Logger logger = LoggerFactory.getLogger(FlightAirFase.class);

    public static void main(String[] args) throws IOException {
        logger.info("FlightAirFase begin");

        String zkUrl = "hadoop01:2181";
        String zkPort = "2181";
        String tableName = "fase_success";
        String colFamily = "cf1";

        //rowCountByScanFilter(zkUrl, zkPort, tableName);
        // 每秒1行，这里取10行
//        long start = 1660712526000L; // time = 1660712526000 key = 5461B 开始
//        long end   = 1660712535000L; // time = 1660724174000 key = 5461B 结束
//        String planeNumber = "B1645";

        // B1870 _ 1660693394000 _ 1660701438000 _timePoint
        long startTime = 1631453855000L;
        long endTime   =  startTime + 500 * 1000; // 1631461370000L;
        String planeNumber = "B30EQ";

        PlaneDetailResponse resp = queryHbase(startTime, endTime, zkUrl, zkPort, tableName, colFamily, planeNumber);

        System.out.println("resp size: " + VM.current().sizeOf(resp));
        System.out.println("resp-size: " + RamUsageEstimator.sizeOf(resp));

        //FileUtil.addContentToFile("json.txt", JSON.toJSONString(resp), "E:\\");

        MemoryMeter meter = new MemoryMeter();
        System.out.println("meter 1: " + meter.measure(resp));
        System.out.println("meter 2: " + meter.measureDeep(resp));

        logger.info("resp = {} - {} / {}", startTime, endTime, resp.getDetails().size());
        logger.info("FlightAirFase end");
    }

    public static PlaneDetailResponse queryHbase(Long start, Long end, String zookeeperIp, String zookeeperPort,
                                                 String hbaseTable, String colFam, String plateNumber) throws IOException {
        logger.info("queryHbase begin");

        //Connection connection = null;
        //PlaneDetailResponse planeDetailResponse = null;
        //Table plane_detail = null;

        //planeDetailResponse = new PlaneDetailResponse();
        LinkedList<PlaneDetail> planeDetails = new LinkedList<PlaneDetail>();

        byte[] startRow = HBaseUtils.resolutionKey2Bytes(new StringBuilder(plateNumber).reverse().toString(), start);
        byte[] endRow = HBaseUtils.resolutionKey2Bytes(new StringBuilder(plateNumber).reverse().toString(), end + 1);

        ResultScanner results = mutiRowBase(zookeeperIp, zookeeperPort, hbaseTable, startRow, endRow);
        int rows = 0;
        for (Result r : results) {
            long time = HBaseUtils.bytes2ResolutionKey(r.getRow())._2;
            String key1 = HBaseUtils.bytes2ResolutionKey(r.getRow())._1;
            rows++;
            if (rows > 0 && rows % 500 == 0) {
                System.out.println("progress " + rows);
            }
            //System.out.println(time);
//            for (Cell cell : r.rawCells()) {
//                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
//                logger.info("time = " + time + " key = " + key1 + " cf = " + cf);
//            }

            PlaneDetail planeDetail = new PlaneDetail();
            planeDetail.setTime(HBaseUtils.bytes2ResolutionKey(r.getRow())._2);
            // 遍历rowkey对应的所有列
            for (Cell cell : r.rawCells()) {
                if (ArrayUtils.isEmpty(CellUtil.cloneValue(cell))) {
                    continue;
                }
                if (colFam.equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                    TreeMap treeMap = BaseUtils.json2Obj(unCompress(CellUtil.cloneValue(cell)), TreeMap.class);
                    HashMap<String, String> stringStringHashMap = new HashMap<>();
                    Set set = treeMap.keySet();
                    for (Object key : set) {
                        stringStringHashMap.put(key.toString(), treeMap.get(key) == null ? "" : treeMap.get(key).toString());
                    }
                    planeDetail.setCollectParamResult(stringStringHashMap);
                    //System.out.println(stringStringHashMap);
                }
            }
            // 添加到返回对象中
            planeDetails.add(planeDetail);
        }

        logger.info("rows = " + rows);
        logger.info("queryHbase end");

        PlaneDetailResponse response = new PlaneDetailResponse();
        response.setPlaneNumber(plateNumber);
        response.setSize(planeDetails.size());
        response.setDetails(planeDetails);
        response.setActualStartTime(start);
        response.setActualEndTime(end);

        return response;
    }

    /**
     * query many row base
     *
     * @param zookeeperIp
     * @param zookeeperPort
     * @param hbaseTable
     * @param startRowkey
     * @param endRowkey
     * @return
     */
    private static ResultScanner mutiRowBase(String zookeeperIp, String zookeeperPort, String hbaseTable, byte[] startRowkey, byte[] endRowkey) {
        Connection connection = null;
        Table myTable = null;
        ResultScanner results = null;
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", zookeeperIp);
            conf.set("hbase.zookeeper.property.clientPort", zookeeperPort);
//            connection = ConnectionFactory.createConnection(conf);
            connection = HBaseUtils.getConnection(zookeeperIp, zookeeperPort);
//            String myTableName = "default:tr";
            String myTableName = hbaseTable;
            myTable = connection.getTable(TableName.valueOf(myTableName));
            Scan scan = new Scan();
            scan.withStartRow(startRowkey);
            scan.withStopRow(endRowkey);
            results = myTable.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (myTable != null) {
                    myTable.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return results;
    }

    public static void rowCountByScanFilter(String zookeeperIp, String zookeeperPort, String tablename) {
        long rowCount = 0;
        try {
            TableName name = TableName.valueOf(tablename);

            //connection为类静态变量
            Connection connection = HBaseUtils.getConnection(zookeeperIp, zookeeperPort);

            //计时
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            Table table = connection.getTable(name);
            Scan scan = new Scan();
            //FirstKeyOnlyFilter只会取得每行数据的第一个kv，提高count速度
            scan.setFilter(new FirstKeyOnlyFilter());
            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs) {
                rowCount += result.size();
            }

            stopWatch.stop();

            System.out.println("RowCount: " + rowCount);
            System.out.println("统计耗时：" + stopWatch.getTime(TimeUnit.MILLISECONDS));
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public static String unCompress(byte[] compressed) throws IOException {
        if (ArrayUtils.isEmpty(compressed)) {
            return null;
        }
        GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8));
        // 使用指定的 charsetName，通过解码字节将缓冲区内容转换为字符串
        return bufferedReader.lines().collect(Collectors.joining());
    }
}
