package com.hadoop.ubcf.job;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

public class JDBCUtils{
	
	private static final Logger logger = LoggerFactory.getLogger(JDBCUtils.class);
	
	public static ConcurrentLinkedQueue<Statement> jdbc_client_pool = new ConcurrentLinkedQueue<Statement>();
	
	private static int jdbc_client_pool_size;
	
	//初始化jdbc连接池
	public static void init(String driver, String url, String username, String password, String pool_size) {
		jdbc_client_pool_size = Integer.parseInt(pool_size);
		Connection con;
		try {
			Class.forName(driver);
			con = DriverManager.getConnection(url, username, password);
			while(--jdbc_client_pool_size>=0) {
				Statement cs = con.createStatement();
				jdbc_client_pool.add(cs);
			}
		} catch (Exception e) {
			try {
				Class.forName(driver);
				con = DriverManager.getConnection(url, username, password);
				while(--jdbc_client_pool_size>=0) {
					Statement cs = con.createStatement();
					jdbc_client_pool.add(cs);
				}
			} catch (Exception e1) {
				logger.info("业务日志:初始化jdbc失败。");
			}
		}
	}
	
	public static List<JSONObject> query(String sql) {
		List<JSONObject> re = new ArrayList<>();
		Statement stat = jdbc_client_pool.poll();
		try (
				ResultSet resultSet = stat.executeQuery(sql);
				){
			ResultSetMetaData metaData = resultSet.getMetaData();
			while(resultSet.next()) {
				JSONObject jsonObject = new JSONObject();
				for(int i = 1; i < metaData.getColumnCount()+1;i++) 
					jsonObject.put(metaData.getColumnName(i), resultSet.getObject(metaData.getColumnName(i)));
				re.add(jsonObject);
			}
		} catch (Exception e) {
			logger.error("--------------");
		}finally {
			jdbc_client_pool.add(stat);
		}
		return re;
	}
}
