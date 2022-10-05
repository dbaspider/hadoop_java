package org.example.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDemo {

    public static void main(String[] args) throws IOException {
        List<String> cfList = new ArrayList<>();
        cfList.add("cf1");
        cfList.add("cf2");

        boolean ret = HBaseUtils.createTable("tab_emp", cfList);
        if (ret) {
            System.out.println("createTable success");
        } else {
            System.out.println("createTable failed");
        }

        HBaseUtils.closeConnection();
    }
}
