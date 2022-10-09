package org.example.hive;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.DriverManager;

// https://sparkbyexamples.com/apache-hive/hive-create-database-from-java-example/
public class HiveCreateDatabase {
	public static void main(String[] args) {
		Connection con = null;
		try {
			String conStr = "jdbc:hive2://192.168.1.148:10000/default";
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			con = DriverManager.getConnection(conStr, "", "");
			Statement stmt = con.createStatement();
			stmt.executeQuery("CREATE DATABASE emp");
			System.out.println("Database emp created successfully.");
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (con != null)
					con.close();
			} catch (Exception ex) {
			}
		}
	}
}
