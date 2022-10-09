package org.example.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @Author: mwb
 * @Date: 2019/12/19 17:25
 */
public class ShellUtils {
    /**
     * 创建shell
     * @param path
     * @param strs
     * @throws Exception
     */
    //@RunLogMethod
    public static void createShell(String path, String... strs) throws Exception {
        System.out.println("createShell path = " + path);

        if (strs == null) {
            System.out.println("strs is null");
            return;
        }

        File sh = new File(path);
        if (sh.exists()) {
            sh.delete();
        }

        sh.createNewFile();
        sh.setExecutable(true);
        FileWriter fw = new FileWriter(sh);
        BufferedWriter bf = new BufferedWriter(fw);

        for (int i = 0; i < strs.length; i++) {
            bf.write(strs[i]);

            if (i < strs.length - 1) {
                bf.newLine();
            }
        }
        bf.flush();
        bf.close();
    }

    /**
     * 执行shell
     * @param shpath String
     * @return String
     * @throws Exception String
     */
    //@RunLogMethod
    public static String runShell(String shpath) throws Exception {
        System.out.println("runShell shpath = " + shpath);

        if (shpath == null || shpath.equals("")) {
            return "shpath is empty";
        }

        Process ps = Runtime.getRuntime().exec(shpath);
        int exitValue = ps.waitFor();

        System.out.println("exitValue = " + exitValue); // ps.exitValue()

        // 标准输出流
        String result = getStream(ps.getInputStream());
        //System.out.println("result = " + getStr(result));

        // 错误输出流
        String resultErr = getStream(ps.getErrorStream());
        System.out.println("result Err = " + getStr(resultErr));

        return result;
    }

    /**
     * getStr
     * @param str String
     * @return String
     */
    public static String getStr(String str) {
        if (str == null) {
            return "<null>";
        } else if (str.length() == 0) {
            return "<empty>";
        }

        return str;
    }

    /**
     * getStream
     * @param stream InputStream
     * @return String
     * @throws IOException When Error
     */
    public static String getStream(InputStream stream) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(stream));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line).append("\n");
        }
        return sb.toString();
    }
}
