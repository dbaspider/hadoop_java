package org.example.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDemo {

    public static void main(String[] args) throws IOException {
        System.out.println("Hbase op begin");

        List<String> cfList = new ArrayList<>();
        cfList.add("cf1");
        cfList.add("cf2");

        boolean ret1 = HBaseUtils.existsTable("tab_emp");
        if (ret1) {
            System.out.println("Table exists");
        } else {
            System.out.println("Table not exists");
            boolean ret = HBaseUtils.createTable("tab_emp", cfList);
            if (ret) {
                System.out.println("createTable success");
            } else {
                System.out.println("createTable failed");
            }
        }

//        boolean added1 = HBaseUtils.putRow("tab_emp", "1001", "cf1", "name", "king");
//        boolean added2 = HBaseUtils.putRow("tab_emp", "1002", "cf1", "name", "jack");
//        boolean added3 = HBaseUtils.putRow("tab_emp", "1002", "cf1", "age", "22");
//        boolean added4 = HBaseUtils.putRow("tab_emp", "1002", "cf2", "school", "Mid 101");
//
//        System.out.println("putRow: " + added1 + " " + added2 + " " + added3 + " " + added4);

        HBaseUtils.closeConnection();

        System.out.println("Hbase op finish");
    }
}
