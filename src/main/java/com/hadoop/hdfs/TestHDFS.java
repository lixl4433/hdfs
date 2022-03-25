package com.hadoop.hdfs;

public class TestHDFS {

	private static String hdfs_upload_file_path = "/test/ubcf";
	private static String local_upload_file_path = "D:/lixl/project/private/hdfs/src/main/config/input.txt";
	
	private static String hdfs_download_file_path = "/test/星空.txt";
	private static String local_download_file_path = "C:/Users/lixl/Desktop/1/1";
	public static void main(String[] args) {
		HDFSUtils.uploadFIleToHDFS(hdfs_upload_file_path, local_upload_file_path);
		
		//HDFSUtils.downloadFileFromHDFS(hdfs_download_file_path, local_download_file_path);
	}

}
