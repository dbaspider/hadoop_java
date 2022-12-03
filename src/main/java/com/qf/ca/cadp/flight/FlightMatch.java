package com.qf.ca.cadp.flight;

import com.qf.ca.cadp.common.utils.HBaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FlightMatch {

    private static final Logger logger = LoggerFactory.getLogger(FlightMatch.class);

    public static void main(String[] args) throws IOException {
        logger.info("FlightMatch begin");

        String zkIp = "hadoop01:2181";
        String zkPort = "2181";

        // B1870_1660693394000_1660701438000_timePoint
        String plateNumber = "B1870";
        long startTime = 1660693394000L;
        long endTime = 1660701438000L;
        String keyName = "timePoint";
        String colFam = "cf1";
        String colName = "map_AIRGND";
        String table = "qar_business3";

        String hbaseRow = HBaseUtils.getHbaseRow(plateNumber, startTime, endTime, keyName, colFam, colName, zkIp, zkPort, table);
        logger.info("hbaseRow = {}", hbaseRow);
    }
}
