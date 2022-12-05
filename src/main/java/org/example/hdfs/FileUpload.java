package org.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.example.utils.HdfsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUpload {

    private static final Logger logger = LoggerFactory.getLogger(FileUpload.class);

    public static void main(String[] args) {
        uploadToHDFS();
    }

    // 测试代码
    private static void uploadToHDFS() {
        logger.info("upload begin");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        //conf.set("fs.defaultFS","hdfs://hadoop01:50090");
        //conf.set("fs.defaultFS","hdfs://hadoop01:39000");
        //conf.set("fs.defaultFS","hdfs://master:8020");
        //conf.set("fs.default.name","hdfs://hadoop01:39000");
        // fs.default.name is deprecated. Instead, use fs.defaultFS
        conf.set("fs.defaultFS","hdfs://hadoop01:39000");
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            HdfsUtil.upload(fileSystem, false, "D:\\tmp_qar\\temp3.txt", "/tmp/");
            //HdfsUtil.download(fileSystem, "/tmp11/qar-json.txt", "D:\\111\\");
            fileSystem.close();
            logger.info("upload ok");
        }catch (Exception Ex) {
            Ex.printStackTrace();
        }
        //System.exit(0);
        logger.info("** finish **");
    }
}
