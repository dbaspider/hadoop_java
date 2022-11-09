package com.qf.ca.cadp.common.utils;

import com.qf.ca.cadp.common.pojo.model.PlaneDetail;
import com.qf.ca.cadp.common.pojo.response.PlaneDetailResponse;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * hbase的rowkey转换工具
 */
public class HBaseUtils {
    public static final int RESOLUTION_KEY_LEN = 12;
    private static final Logger logger = LoggerFactory.getLogger(HBaseUtils.class);
    /**
     * byte转回原来的格式
     *
     * @param rowkey
     * @return
     */
    public static Tuple2<String, Long> bytes2ResolutionKey(byte[] rowkey) {
        /*if(ArrayUtils.getLength(rowkey) != 12){
            System.out.println("[bytes2ResolutionKey] wrong key length: " + ArrayUtils.getLength(rowkey));
            return null;
        }*/

//        System.out.println(" key length: " + ArrayUtils.getLength(rowkey));
        byte[] keyStr = ArrayUtils.subarray(rowkey, 0, ArrayUtils.getLength(rowkey) - 8);
        byte[] keyLong = ArrayUtils.subarray(rowkey, ArrayUtils.getLength(rowkey) - 8, ArrayUtils.getLength(rowkey));
        return new Tuple2<String, Long>(Bytes.toString(keyStr), Bytes.toLong(keyLong));
    }

    /**
     *  resolution rowkey to three parameter(String(2),Long(8),String)
     * @param rowkey
     * @return
     */
    public static Tuple3<String,Long,String> bytes2ResolutionKey3(byte[] rowkey) {
        String org = Bytes.toString(ArrayUtils.subarray(rowkey, 0, 2));
        Long flight = Bytes.toLong(ArrayUtils.subarray(rowkey, 2, 10));
        String point = Bytes.toString(ArrayUtils.subarray(rowkey, 10, ArrayUtils.getLength(rowkey)));
        return new Tuple3<String,Long,String>(org,flight,point);
    }

    /**
     * 转为byte(rowkey包含2个)
     *
     * @param planeNumber
     * @param time
     * @return
     */
    public static byte[] resolutionKey2Bytes(String planeNumber, long time) {
        byte[] plane = Bytes.toBytes(planeNumber);
        byte[] t = Bytes.toBytes(time);
        byte[] planet = ArrayUtils.addAll(plane, t);
        return planet;
    }

    /**
     *  generate rowkey by three parameter(String(2),Long(8),String)
     * @param planeNumber
     * @param flightId
     * @param flightPoint
     * @return
     */
    public static byte[] resolutionKey2Bytes(String planeNumber, long flightId, String flightPoint) {
        return ArrayUtils.addAll(ArrayUtils.addAll(Bytes.toBytes(planeNumber), Bytes.toBytes(flightId)), Bytes.toBytes(flightPoint));
    }



    public static Connection createConnection(String zkUrl, String port) throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", zkUrl);
        conf.set("hbase.zookeeper.property.clientPort", port);
        Connection connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

    public static final Map<String, Connection> connectionCache = new ConcurrentHashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            connectionCache.forEach((k, v) -> {
                try {
                    v.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            });
        }));
    }

    public static Connection getConnection(String zkUrl, String port) {
        return connectionCache.compute(zkUrl + "." + port, (k, oldConn) -> {
            if (oldConn == null || oldConn.isClosed()) {
                try {
                    oldConn = createConnection(zkUrl, port);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }
            return oldConn;
        });

    }


    public static void main(String[] args) {
        /*String temp = "b737";
        byte[] bytes = Bytes.toBytes(temp);
        System.out.println(ArrayUtils.getLength(bytes));
        byte[] bytes1 = Bytes.toBytes(158999787878L);
        System.out.println(ArrayUtils.getLength(bytes1));*/

    }


    /**
     * 提交多条数据 不同rowKey 同一列族 不同列
     * @param zk
     * @param zkPort
     * @param tableName
     * @param map
     * @param family
     */
    public static void putMultiRow(String zk , String zkPort , String tableName , Map<byte[] , HashMap<String ,Long>>  map , String family){

        Connection conn = HBaseUtils.getConnection(zk, zkPort);
        Table table = null ;
        List<Put> puts = new ArrayList<>();
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Iterator<Map.Entry<byte[], HashMap<String, Long>>> iterator = map.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<byte[], HashMap<String, Long>> next = iterator.next();
                byte[] rowkey = next.getKey() ;
                Put put = new Put(rowkey);
                HashMap<String, Long> hashMap = next.getValue();
                Iterator<Map.Entry<String, Long>> iter = hashMap.entrySet().iterator();
                while (iter.hasNext()){
                    Map.Entry<String, Long> param = iter.next();
                    String col = param.getKey();
                    Long  value = param.getValue();
                    put.addColumn(Bytes.toBytes(family) , Bytes.toBytes(col) , Bytes.toBytes(value));
                    puts.add(put);
                    try{
                        Tuple2<String, Long> tp = HBaseUtils.bytes2ResolutionKey(rowkey);
                        logger.warn("rowkey : {} , col : {} , value : {} " , tp._1 + "_" + tp._2 , col , value);
                    }catch (Exception e){
                        logger.error(e.getMessage() + "------------" + col + "," +value );
                    }
                }
            }
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(table != null){
                try {
                    table.close();
                    table = null ;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * 提交多条数据 不同rowKey 同一列族 不同列
     * @param zk
     * @param zkPort
     * @param tableName
     * @param map
     * @param family
     */
    public static void putMultiStringRow(String zk , String zkPort , String tableName , Map<byte[] , HashMap<String ,String>>  map , String family){

        Connection conn = HBaseUtils.getConnection(zk, zkPort);
        Table table = null ;
        List<Put> puts = new ArrayList<>();
            try {
            table = conn.getTable(TableName.valueOf(tableName));
            Iterator<Map.Entry<byte[], HashMap<String, String>>> iterator = map.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<byte[], HashMap<String, String>> next = iterator.next();
                byte[] rowkey = next.getKey() ;
                Put put = new Put(rowkey);
                HashMap<String, String> hashMap = next.getValue();
                Iterator<Map.Entry<String, String>> iter = hashMap.entrySet().iterator();
                while (iter.hasNext()){
                    Map.Entry<String, String> param = iter.next();
                    String col = param.getKey();
                    String  value = param.getValue();
                    put.addColumn(Bytes.toBytes(family) , Bytes.toBytes(col) , Bytes.toBytes(value));
                    puts.add(put);
                    try{
                        Tuple2<String, Long> tp = HBaseUtils.bytes2ResolutionKey(rowkey);
                        logger.warn("rowkey : {} , col : {} , value : {} " , tp._1 + "_" + tp._2 , col , value);
                    }catch (Exception e){
                        logger.error(e.getMessage() + "------------" + col + "," +value );
                    }
                }
            }
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(table != null){
                try {
                    table.close();
                    table = null ;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static ConcurrentHashMap getRow(Connection conn, String tableName, String OrgCode, Long segmentId){
        Table table = null;
        ConcurrentHashMap familyAndValues = new ConcurrentHashMap<String,String>();
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            byte[] rowkey = HBaseUtils.resolutionKey2Bytes(OrgCode.toUpperCase(), segmentId);
            Get get = new Get(rowkey);
            Result result = table.get(get);
            Cell[] cells = result.rawCells();

            for(Cell cell : cells){
                familyAndValues.put(Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if (table !=null){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return familyAndValues;
    }
    public static ConcurrentHashMap scanRows(Connection conn, String tableName, String OrgCode, Long startSegmentId, Long stopSegmentid){
        Table table = null;
        ConcurrentHashMap rowkeyAndRows = new ConcurrentHashMap<Object,String>();
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            byte[] startKey = HBaseUtils.resolutionKey2Bytes(OrgCode.toUpperCase(), startSegmentId);
            byte[] stopKey = HBaseUtils.resolutionKey2Bytes(OrgCode.toUpperCase(), stopSegmentid);

            Scan scan = new Scan();
            scan.withStartRow(startKey);
            scan.withStopRow(stopKey);
            scan.setCacheBlocks(false);

            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext()){
                ConcurrentHashMap familyAndValues = new ConcurrentHashMap<String,String>();
                Result next = iterator.next();
                for (Cell cell:next.rawCells() ) {
                    familyAndValues.put(Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                    rowkeyAndRows.putIfAbsent(CellUtil.cloneRow(cell),familyAndValues);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if (table !=null){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return rowkeyAndRows;
    }
    /**
     * 保存一行数据  同一列族 同一rowkey 不同列的值
     * @param zk
     * @param zkPort
     * @param tableName
     * @param rowkey
     * @param map
     * @param family
     */
    public static void putRow(String zk , String zkPort , String tableName , byte[] rowkey , Map<String , String> map , String family){

        Connection conn = HBaseUtils.getConnection(zk, zkPort);
        Table table = null ;
        List<Put> puts = new ArrayList<>();
        Put put  = new Put(rowkey);
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, String> next = iterator.next();
                String key = next.getKey();
                String value = next.getValue();
                put.addColumn(Bytes.toBytes(family) , Bytes.toBytes(key) , Bytes.toBytes(value));
                puts.add(put);
                logger.warn("key : {} , value : {} " , key , value);
            }
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(table != null){
                try {
                    table.close();
                    table = null ;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     *  根据飞机号、起止时间查询译码结果
     * @param planeNumber
     * @param start
     * @param end
     * @param zk
     * @param port
     * @param tableName
     * @param colFam
     * @return
     */
    public static PlaneDetailResponse queryPlaneDetail(String planeNumber, Long start, Long end, String zk, String port, String tableName, String colFam) {
        Connection connection = null;
        PlaneDetailResponse planeDetailResponse = null;
        Table table = null;
        try {
            planeDetailResponse = new PlaneDetailResponse();
            LinkedList<PlaneDetail> planeDetails = new LinkedList<PlaneDetail>();
            connection = HBaseUtils.getConnection(zk,port);
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.withStartRow(HBaseUtils.resolutionKey2Bytes(new StringBuilder(planeNumber).reverse().toString(), start));
            scan.withStopRow(HBaseUtils.resolutionKey2Bytes(new StringBuilder(planeNumber).reverse().toString(), end + 1));
            scan.setCacheBlocks(false);
            ResultScanner results = table.getScanner(scan);

            for (Result r : results) {
                PlaneDetail planeDetail = new PlaneDetail();
                planeDetail.setTime(HBaseUtils.bytes2ResolutionKey(r.getRow())._2);
                HashMap<String, String> custom = new HashMap<>();
                for (Cell cell : r.rawCells()) {
                    if (colFam.equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                        TreeMap treeMap = BaseUtils.json2Obj(unCompress(CellUtil.cloneValue(cell)), TreeMap.class);
                        HashMap<String, String> stringStringHashMap = new HashMap<>();
                        Set set = treeMap.keySet();
                        for (Object key : set) {
                            stringStringHashMap.put(key.toString(), treeMap.get(key) == null ? "" : treeMap.get(key).toString());
                        }
                        planeDetail.setCollectParamResult(stringStringHashMap);
                    }else if("1".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                        custom.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }
                planeDetail.setCustomParamResult(custom);
                planeDetails.add(planeDetail);
            }
            planeDetailResponse.setPlaneNumber(planeNumber);
            planeDetailResponse.setDetails(planeDetails);
            planeDetailResponse.setSize(planeDetails.size());
            planeDetailResponse.setActualStartTime(start);
            planeDetailResponse.setActualEndTime(end);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table!=null){
                try {
                    table.close();
                    table = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return planeDetailResponse;
    }


    public static String unCompress(byte[] compressed) throws IOException {
        if (ArrayUtils.isEmpty(compressed)) {
            return null;
        }
        GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gis, "utf-8"));
        String str = bufferedReader.lines().collect(Collectors.joining());
        return str;
    }
}
