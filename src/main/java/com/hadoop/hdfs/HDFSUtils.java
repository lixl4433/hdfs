package com.hadoop.hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hadoop.ubcf.job.ReadProperties;

public class HDFSUtils {
	public static String HDFS_URL = null;
	private static final Logger logger = LoggerFactory.getLogger(HDFSUtils.class);
	
	public static void init() {
		HDFS_URL = ReadProperties.get("com.hadoop.hdfs.url");
	}
	
	/**
	 * @param hdfs_file_path    /xxx/xxx/xxx     will upload from hdfs to local ,  this is hdfs file path.
	 * @param local_file_path   /xxx/xxx/xx.txt  localfile path
	 */
	public static void uploadFIleToHDFS(String hdfs_file_path, String local_file_path) {
		Configuration conf = new Configuration();
		Path localPath = new Path(local_file_path);
        Path hdfsPath = new Path(HDFS_URL+hdfs_file_path);
		try {
			FileSystem hdfs = FileSystem.get(conf);
			if(!hdfs.exists(hdfsPath)){
				hdfs.mkdirs(hdfsPath);
			}
			hdfs.copyFromLocalFile(localPath, hdfsPath);
		} catch (IOException e) {
			logger.info("upload failed");
		}
	}
	
	
	/**
	 * 
	 * @param hdfs_file_path      /xxx/xxx/xxx/xxx.txt    hdfs file path
	 * @param local_file_path     /xxx/xxx/xxx            will download local file path
	 */
	public static void downloadFileFromHDFS(String hdfs_file_path, String local_file_path) {
		Configuration conf = new Configuration();
		String file_name = hdfs_file_path.split("/")[hdfs_file_path.split("/").length-1];
		File f = new File(local_file_path);
		try {
			if(!f.exists())
				f.mkdir();
			File ff = new File(local_file_path+"/"+file_name);
			if(!ff.exists())
					f.createNewFile();
			try (
					FileSystem hdfs = FileSystem.get(URI.create(HDFS_URL+hdfs_file_path), conf);
					BufferedInputStream hdfs_input = new BufferedInputStream(hdfs.open(new Path(HDFS_URL+hdfs_file_path)));
					BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(new File(local_file_path+"/"+file_name)));
					){
				
				int data = -1;
				while((data = hdfs_input.read()) != -1) {
					bos.write(data);
				}
			} catch (IOException e) {
				logger.info("download failed");
			}
		} catch (IOException e1) {
			logger.info("download failed");
		}
	}
	
	/**
	 * 获取hdfs文件流
	 * @param hdfs_file_path
	 * @return
	 */
	public static BufferedInputStream hdfsInputStream(String hdfs_file_path) {
		try {
			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(URI.create(HDFS_URL + hdfs_file_path), conf);
			BufferedInputStream hdfs_input = new BufferedInputStream(hdfs.open(new Path(HDFS_URL + hdfs_file_path)));
			return  hdfs_input;
		} catch (Exception e) {
			logger.info("get hdfs input stream failed");
		}
		return null;
	}
	
	public static void mergeHDFSFile(String hdfs_file_dir_path, String to_file_path) {
		BufferedReader br = null;
		Configuration configuration = new Configuration();
		try (
				FileSystem hdfs = FileSystem.get(configuration);
				FSDataOutputStream fsDataOutputStream = hdfs.create(new Path(to_file_path));
				Writer writer = new OutputStreamWriter(fsDataOutputStream, "UTF-8");
				BufferedWriter bufferedWriter = new BufferedWriter(writer);
				){
			FileStatus[] status = hdfs.listStatus(new Path(hdfs_file_dir_path));
			for (FileStatus file : status) {
				if (!file.getPath().getName().startsWith("part-")) {
					continue;
				}

				FSDataInputStream inputStream = hdfs.open(file.getPath());
				br = new BufferedReader(new InputStreamReader(inputStream));

				String line = null;
				while (null != (line = br.readLine())) {
					bufferedWriter.write(line + "\n");
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				if(null != br)
					br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void mkFile(String fullFilePath) {
		Configuration conf = new Configuration();
        Path hdfsPath = new Path(fullFilePath);
		try {
			FileSystem hdfs = FileSystem.get(conf);
			if(!hdfs.exists(hdfsPath)){
				hdfs.mkdirs(hdfsPath);
			}
		} catch (IOException e) {
			logger.info("upload failed");
		}
	}
	
}
