package com.hadoop.ubcf.job;

import java.util.List;

import com.alibaba.fastjson.JSONObject;

public class ReadInput {
	
	public void read() {
		
		String driver = ReadProperties.get("com.hadoop.input.jdbc.driver");
		String url = ReadProperties.get("com.hadoop.input.jdbc.url");
		String username = ReadProperties.get("com.hadoop.input.jdbc.username");
		String password = ReadProperties.get("com.hadoop.input.jdbc.password");
		String pool_size = ReadProperties.get("com.hadoop.input.jdbc.pool_size");
		String sql = ReadProperties.get("com.hadoop.input.jdbc.sql");
		
		JDBCUtils.init(driver, url, username, password, pool_size);;
		List<JSONObject> datas = JDBCUtils.query(sql);
		
		
		
	}
}
