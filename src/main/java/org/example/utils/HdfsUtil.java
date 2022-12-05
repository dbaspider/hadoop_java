package org.example.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

@Slf4j
public class HdfsUtil {

	public static FileSystem getFileSystem(DfsConfig dfsConfig) {
		log.info("[HdfsUtil::getFileSystem] with param: {}", dfsConfig);
		try {
			System.setProperty("HADOOP_USER_NAME", dfsConfig.getHadoopUsername());
			Configuration configuration = new Configuration();
			configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			configuration.set("dfs.client.use.datanode.hostname", dfsConfig.getUseDataNodeHostName());
			configuration.set("fs.defaultFS", dfsConfig.getDefaultFS());
			return FileSystem.get(configuration);
		} catch (Exception ex) {
			log.warn("[HdfsUtil::getFileSystem] Exception: {}", ex.getMessage());
		}
		return null;
	}

	/**
	 * @param delSrc 为true时，会删除原文件
	 * @param src 为文件时，上传文件；为目录时，上传目录下所有文件
	 * @param dst 为HDFS的目录，如果目不存在，会先创建
	 * 文件上传到HDFS
	 */
	public static void upload(FileSystem fs, boolean delSrc, String src, String dst) {
		log.info("[HdfsUtil::upload] with params: fs={}, delSrc={}, src={}, dst={}", fs, delSrc, src, dst);
		try {
			if (Objects.nonNull(fs)) {
				forceMkdir(fs, dst);
				fs.copyFromLocalFile(delSrc, new Path(src), new Path(dst));
			}
		} catch (Exception ex) {
			log.warn("[HdfsUtil::upload] Exception: {}", ex.getMessage());
			ex.printStackTrace();
		}
	}

	/**
	 * @param src 为文件时，下载文件；为目录时，下载目录下所有文件
	 * @param dst 为本地的目录，如果目不存在，会先创建
	 * 下载HDFS文件
	 */
	public static void download(FileSystem fs, String src, String dst) {
		log.info("[HdfsUtil::download] with params: fs={}, src={}, dst={}", fs, src, dst);
		try {
			if (Objects.nonNull(fs)) {
				FileUtils.forceMkdir(new File(dst));
				fs.copyToLocalFile(false, new Path(src), new Path(dst));
			}
		} catch (Exception ex) {
			log.warn("[HdfsUtil::download] Exception: {}", ex.getMessage());
		}
	}

	/**
	 * 删除HDFS上文件
	 */
	public static boolean delete(FileSystem fs, String path) {
		log.info("[HdfsUtil::delete] with params: fs={}, path={}", fs, path);
		try {
			if (Objects.nonNull(fs)) {
				return fs.delete(new Path(path), true);
			}
		} catch (Exception ex) {
			log.warn("[HdfsUtil::delete] Exception: {}", ex.getMessage());
		}
		return false;
	}

	public static void close(FileSystem fs) {
		log.info("[HdfsUtil::close] with param: fs={}", fs);
		try {
			if (Objects.nonNull(fs)) {
				fs.close();
			}
		} catch (Exception ex) {
			log.warn("[HdfsUtil::close] Exception: {}", ex.getMessage());
		}
	}

	private static void forceMkdir(FileSystem fs, String dst) throws IOException {
		String message;
		Path hdfsPath = new Path(dst);
		if (fs.exists(hdfsPath)) {
			if (!fs.getFileStatus(hdfsPath).isDirectory()) {
				message = "File " + dst + " exists and is not a directory. Unable to create directory.";
				throw new IOException(message);
			}
		} else if (!fs.mkdirs(hdfsPath) && !fs.getFileStatus(hdfsPath).isDirectory()) {
			message = "Unable to create directory " + dst;
			throw new IOException(message);
		}

	}

}
