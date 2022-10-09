package org.example.hive;

import org.example.utils.ShellUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HiveShell {

    public static void main(String[] args) throws Exception {
        System.out.println("========= HiveShell begin =========");

        String shellPath = "/mnt/hgfs/centos_7_share2/loadDataShell.sh";

        String hiveCommand = "#!/bin/bash\n" +
                "hive -e \" CREATE TABLE IF NOT exists tb_part_emp\n" +
                "(\n" +
                "\tEname STRING,\n" +
                "\tEmpID INT,\n" +
                "\tSalary FLOAT,\n" +
                "\tbirthday STRING\n" +
                ")\n" +
                "PARTITIONED BY (country STRING); \"";

        ShellUtils.createShell(shellPath, hiveCommand);
        String output = ShellUtils.runShell(shellPath);
        System.out.println("output >> " + output);

        System.out.println("========= HiveShell finish =========");

        testCase();

        execCommand();
    }

    public static void testCase() throws Exception {
        System.out.println("========= testCase begin =========");

        String shellPath = "/mnt/hgfs/centos_7_share2/dirShell.sh";

        String hiveCommand = "#!/bin/bash\n" +
                "ls -al\n";

        ShellUtils.createShell(shellPath, hiveCommand);
        String output = ShellUtils.runShell(shellPath);
        System.out.println("output >> " + output);

        System.out.println("========= testCase finish =========");
    }

    private static void execCommand() {
        //String[] command = {"echo", "Hello", "world."};
        String[] command = {"ll", "-a"};
        new Thread(() -> {
            System.out.println("========= Thread begin =========");
            try {
                ProcessBuilder builder = new ProcessBuilder(command);
                builder.redirectErrorStream(true);
                final Process proc = builder.start();
                int ret = proc.waitFor();
                System.out.println("ret = " + ret);
                BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                String s = null;
                while ((s = in.readLine()) != null) {
                    System.out.println(s);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("========= Thread finish =========");
        }).start();
    }
}
