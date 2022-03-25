package com.hadoop.ubcf.step4;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.util.concurrent.AtomicDouble;
import com.hadoop.hdfs.HDFSUtils;
 
/**
 * 
 * 用户与用户相似度矩阵X评分矩阵（经过步骤3转置）
 */
public class Mapper4 extends Mapper<LongWritable, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text();
	private List<String> cacheList = new ArrayList<>();
	private static ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<>();
	/*	private static ConcurrentHashMap<String, String> cache1 = new ConcurrentHashMap<>();
		private static ConcurrentHashMap<String, String> cache2 = new ConcurrentHashMap<>();
		private static ConcurrentHashMap<String, String> cache3 = new ConcurrentHashMap<>();
		private static ConcurrentHashMap<String, String> cache4 = new ConcurrentHashMap<>();
		private static ConcurrentHashMap<String, String> cache5 = new ConcurrentHashMap<>();
		private static ConcurrentHashMap<String, String> cache6 = new ConcurrentHashMap<>();
		private static ConcurrentHashMap<String, String> cache7 = new ConcurrentHashMap<>();
		private static ConcurrentHashMap<String, String> cache8 = new ConcurrentHashMap<>();
		private static ConcurrentHashMap<String, String> cache9 = new ConcurrentHashMap<>();*/
	//private AtomicInteger count = new AtomicInteger(0);
	
	private DecimalFormat df = new DecimalFormat("0.00");
		
	/**在map执行之前会执行这个方法，只会执行一次
	 * 
	 * 通过输入流将全局缓存中的矩阵读入一个java容器中
	 */
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		super.setup(context);
		
		String path = HDFSUtils.HDFS_URL + context.getCacheArchives()[0].getPath();
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(path), conf);
		FSDataInputStream fsdi = hdfs.open(new Path(path));
		BufferedReader br = new BufferedReader(new InputStreamReader(fsdi, "UTF-8"));

		
		//右矩阵	
		String line = null;
		while ((line = br.readLine()) != null) {
			cacheList.add(line);
			//      物品
			//用户
			//1     a_3.00,b_3.00,c_4.00
			//2     a_3.00,b_3.00,c_4.00
			//3     a_3.00,b_3.00,c_4.00
			//
			//数据封装读入内存  ------------->
			//
			//{
			//   "1_a":"3.00",
			//   "1_b":"3.00",
			//   "1_c":"4.00",
			//   "2_a":"3.00",
			//   "2_b":"3.00",
			//   "2_c":"4.00",
			//   "3_a":"3.00",
			//   "3_b":"3.00",
			//   "3_c":"4.00"
			//}
			
			String[] lineArr = line.toString().split("\t");
			String goods = lineArr[0];
			String user_scores = lineArr[1];
			String[] user_scores_arr = user_scores.split(",");
			Arrays.asList(user_scores_arr).parallelStream().forEach(user_score ->{
				String[] userAndScore = user_score.split("_");
				String user = userAndScore[0];
				String score = userAndScore[1];
				cache.put(user+"_"+goods, score);
			});
		}
		
		br.close();
	}
 
 
	/**
	 * key: 行号	物品ID
	 * value:行	列_值,列_值,列_值,列_值	用户ID_分值
	 * */
    @Override  
	protected void map(LongWritable key, Text value, Context context){
		String[] row1 = value.toString().split("\t");
		String user1 = row1[0];
		String[] user2_scores = row1[1].split(",");
		for (String line : cacheList) {
			String[] row2 = line.toString().split("\t");
			String goods2 = row2[0];
			AtomicDouble result = new AtomicDouble(0);
			Arrays.asList(user2_scores).parallelStream().forEach(user2_score -> {
				double result_tmp = result.get();
				String user2 = user2_score.split("_")[0];
				String score1 = user2_score.split("_")[1];
				String score2 = cache.get(user2 + "_" + goods2);
				result_tmp += Double.valueOf(score1) * Double.valueOf(score2);
				result.set(result_tmp);
			});
			if (result.get() == 0)
				continue;
			String k = user1;
			String v = goods2 + "_" + df.format(result.get());
			outKey.set(k);
			outValue.set(v);
			try {
				context.write(outKey, outValue);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			/*			if(count.getAndIncrement()%1000 == 0) {
							System.out.println(count.getAndIncrement() + "       "+k + "----------->" + v);
						}
			*/			
		}
	}
    
    
    
	/* public static void putKV(String user, String goods, String score) {
		switch (Integer.parseInt(user) % 10) {
		case 0:
			cache.put(user+"_"+goods, score);
			break;
		case 1:
			cache1.put(user+"_"+goods, score);
			break;
		case 2:
			cache2.put(user+"_"+goods, score);
			break;
		case 3:
			cache3.put(user+"_"+goods, score);
			break;
		case 4:
			cache4.put(user+"_"+goods, score);
			break;
		case 5:
			cache5.put(user+"_"+goods, score);
			break;
		case 6:
			cache6.put(user+"_"+goods, score);
			break;
		case 7:
			cache7.put(user+"_"+goods, score);
			break;
		case 8:
			cache8.put(user+"_"+goods, score);
			break;
		case 9:
			cache9.put(user+"_"+goods, score);
			break;
		default:
			break;
		}
	}
	
	public ConcurrentHashMap<String, String> getCache(String user){
		switch (Integer.parseInt(user) % 10) {
		case 0:
			return cache;
		case 1:
			return cache1;
		case 2:
			return cache2;
		case 3:
			return cache3;
		case 4:
			return cache4;
		case 5:
			return cache5;
		case 6:
			return cache6;
		case 7:
			return cache7;
		case 8:
			return cache8;
		case 9:
			return cache9;
		default:
			return null;
		}
	}*/
}
