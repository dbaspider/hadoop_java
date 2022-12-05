package org.example.utils;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

@Data
//@Configuration
public class DfsConfig {

	//@Value("${dfs.local.tempDir:}")
	private String localTempDir;

	//@Value("${dfs.hdfs.tempDir:}")
	private String hdfsTempDir;

	//@Value("${dfs.client.use.datanode.hostname:true}")
	private String useDataNodeHostName;

	//@Value("${dfs.defaultFS:}")
	private String defaultFS;

	//@Value("${dfs.hadoop.username:}")
	private String hadoopUsername;

	public String getLocalTempDir() {
		if (StringUtils.isEmpty(localTempDir)) {
			return "./";
		}
		if (!localTempDir.endsWith("/")) {
			return localTempDir.concat("/");
		}
		return localTempDir;
	}

	public String getHdfsTempDir() {
		if (StringUtils.isEmpty(hdfsTempDir)) {
			return "./";
		}
		if (!hdfsTempDir.endsWith("/")) {
			return hdfsTempDir.concat("/");
		}
		return hdfsTempDir;
	}
}
