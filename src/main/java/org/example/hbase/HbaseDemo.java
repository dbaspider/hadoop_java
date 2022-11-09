package org.example.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDemo {

    private static final Logger logger = LoggerFactory.getLogger(HbaseDemo.class);

    public static void main(String[] args) throws IOException {
        logger.info("Hbase op begin");

        List<String> cfList = new ArrayList<>();
        cfList.add("cf1");
        cfList.add("cf2");

        HBaseUtilsOrg.createConnection();

        boolean ret1 = HBaseUtilsOrg.existsTable("tab_stu");
        if (ret1) {
            logger.info("Table already exists");
        } else {
            logger.info("Table not exists, will create it");
            boolean ret = HBaseUtilsOrg.createTable("tab_stu", cfList);
            if (ret) {
                logger.info("create Table success");
            } else {
                logger.info("create Table failed");
            }
        }

//        boolean added1 = HBaseUtilsOrg.putRow("tab_emp", "1001", "cf1", "name", "king");
//        boolean added2 = HBaseUtilsOrg.putRow("tab_emp", "1002", "cf1", "name", "jack");
//        boolean added3 = HBaseUtilsOrg.putRow("tab_emp", "1002", "cf1", "age", "22");
//        boolean added4 = HBaseUtilsOrg.putRow("tab_emp", "1002", "cf2", "school", "Mid 101");
//
//        System.out.println("putRow: " + added1 + " " + added2 + " " + added3 + " " + added4);

        HBaseUtilsOrg.closeConnection();

        logger.info("Hbase op finish");
    }
}
