package org.example.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class HiveDemo {

    private static String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static String CONNECTION_URL ="jdbc:hive2://localhost:10000/";
    // Connection conn = DriverManager.getConnection("jdbc:hive2://127.0.0.1:10000/default","hadoop","");

    static {
        System.out.println("load jdbc driver ...");
        try {
            Class.forName(JDBC_DRIVER);
            System.out.println("load jdbc driver ok ...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("JDBC to HiveServer2");
        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            connection = DriverManager.getConnection(CONNECTION_URL);

            ps = connection.prepareStatement("select * from tb_dept");
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getInt(1)
                        + "-------" + rs.getString(2)
                        + "-------" + rs.getString(3));
            }
            rs.close();
            ps.close();

            System.out.println("---------------------------------------------");

            ps = connection.prepareStatement("select * from tb_emp");
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getInt(1)
                        + "-------" + rs.getString(2)
                        + "-------" + rs.getString(3)
                        + "-------" + rs.getString(4)
                        + "-------" + rs.getString(5)
                        + "-------" + rs.getString(6)
                        + "-------" + rs.getString(7)
                        + "-------" + rs.getString(8));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.disconnect(connection, rs, ps);
        }
    }
}
