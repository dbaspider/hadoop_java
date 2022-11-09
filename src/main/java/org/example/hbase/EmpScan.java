package org.example.hbase;

import com.qf.ca.cadp.common.utils.HBaseUtils;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class EmpScan {

    private static final Logger logger = LoggerFactory.getLogger(EmpScan.class);

    private static Connection g_Connection = null;

    public static void main(String[] args) throws IOException {
        logger.info("EmpScan begin");

        String zkUrl = "hadoop01:2181";
        String zkPort = "2181";
        String tableName = "tab_emp";

        String start = "1001";
        String end   = "1003";

        queryHbase(start, end, zkUrl, zkPort, tableName);

        logger.info("EmpScan end");
    }

    public static void queryHbase(String start, String end, String zookeeperIp, String zookeeperPort, String hbaseTable) throws IOException {
        logger.info("queryHbase begin");

        byte[] startRow = Bytes.toBytes(start);
        byte[] endRow = Bytes.toBytes(end);

        ResultScanner results = mutiRowBase(zookeeperIp, zookeeperPort, hbaseTable, startRow, endRow);
        int rows = 0;
        for (Result r : results) {
            rows++;

            // 遍历rowkey对应的所有列
            for (Cell cell : r.rawCells()) {
                if (ArrayUtils.isEmpty(CellUtil.cloneValue(cell))) {
                    continue;
                }

                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                String val = Bytes.toString(CellUtil.cloneValue(cell));

                logger.info("row {} : {} {} {}", rows, cf, col, val);
            }
        }

        logger.info("*** rows = " + rows);
        logger.info("queryHbase end");
    }

    /**
     * query many row base
     *
     * @param zookeeperIp
     * @param zookeeperPort
     * @param hbaseTable
     * @param startRowkey
     * @param endRowkey
     * @return ResultScanner
     */
    private static ResultScanner mutiRowBase(String zookeeperIp, String zookeeperPort, String hbaseTable, byte[] startRowkey, byte[] endRowkey) {
        Connection connection = null;
        Table myTable = null;
        ResultScanner results = null;
        try {
            connection = getConnection(zookeeperIp, zookeeperPort);
            myTable = connection.getTable(TableName.valueOf(hbaseTable));
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

    private static Connection getConnection(String zookeeperIp, String zookeeperPort) {
        logger.info("getConnection");
        if (g_Connection == null) {
            logger.info("getConnection : create a new conn >>");
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", zookeeperIp);
            conf.set("hbase.zookeeper.property.clientPort", zookeeperPort);
            g_Connection = HBaseUtils.getConnection(zookeeperIp, zookeeperPort);
            logger.info("getConnection : create a new conn <<");
        }
        return g_Connection; // 直接用全局的即可
    }

    private static void closeConnection() {
        logger.info("closeConnection");
        if (g_Connection != null) {
            try {
                g_Connection.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}
