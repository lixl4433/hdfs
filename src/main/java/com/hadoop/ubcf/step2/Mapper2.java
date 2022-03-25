package com.hadoop.ubcf.step2;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
 * 利用评分矩阵构建用户与用户的相似度矩阵
 */
public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text();
	private List<String> cacheList = new ArrayList<String>();
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
		
		/*String fname = context.getJobID()+"_"+context.getTaskAttemptID()+"_"+System.currentTimeMillis();
		Configuration configuration = context.getConfiguration();
		String outpath = configuration.get("mapreduce.task.output.dir");
		String s3 = outpath +"/"+fname;
		HDFSUtils.mkFile(s3);*/
		
		//右矩阵	
		//key:行号 物品ID		value:列号_值,列号_值,列号_值,列号_值,列号_值...    用户ID_分值
		String line = null;
		while((line=br.readLine())!=null){
			cacheList.add(line);
			
			String[] lineArr = line.toString().split("\t");
			String user = lineArr[0];
			String goods_scores = lineArr[1];
			String[] goods_scores_arr = goods_scores.split(",");
			Arrays.asList(goods_scores_arr).parallelStream().forEach(goods_score ->{
				String[] goodsAndScore = goods_score.split("_");
				String goods = goodsAndScore[0];
				String score = goodsAndScore[1];
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
	protected void map(LongWritable key, Text value, Context context) {

		String[] row = value.toString().split("\t");

		// 矩阵行号
		String userId = row[0];
		// 列_值
		String[] goods_scores = row[1].split(",");

		// 计算左侧矩阵行的空间距离
		double denominator1 = 0;
		for (String goods_score : goods_scores) {
			String score = goods_score.split("_")[1];
			denominator1 += Double.valueOf(score) * Double.valueOf(score);
		}

		List<Double> denominators1 = new ArrayList<>();
		denominators1.add(Math.sqrt(denominator1));

		cacheList.forEach(line -> {
			String[] row2 = line.toString().split("\t");
			// 右侧矩阵line
			// 格式: 列 tab 行_值,行_值,行_值,行_值
			String userId2 = row2[0];
			String[] goods_scores2 = row2[1].split(",");

			// 计算右侧矩阵行的空间距离

			AtomicDouble AtomDenominator2 = new AtomicDouble(0);
			Arrays.asList(goods_scores2).parallelStream().forEach(goods_score2 -> {
				double denominator2_tmp = AtomDenominator2.get();
				String score = goods_score2.split("_")[1];
				denominator2_tmp += Double.valueOf(score) * Double.valueOf(score);
				AtomDenominator2.set(denominator2_tmp);
			});
			double denominator2 = Math.sqrt(AtomDenominator2.get());

			// 矩阵两位相乘得到的结果 分子

			AtomicInteger atomicNumerator = new AtomicInteger(0);
			// 遍历左侧矩阵一行的每一列

			Arrays.asList(goods_scores).parallelStream().forEach(goods_score -> {
				int numerator_tmp = atomicNumerator.get();
				String goodsId = goods_score.split("_")[0];
				String score = goods_score.split("_")[1];
				
				String score2 = cache.get(userId2+"_"+goodsId);
				// 将两列的值相乘并累加
				numerator_tmp += Integer.valueOf(score) * Integer.valueOf(score2);
				atomicNumerator.set(numerator_tmp);
			});

			int numerator = atomicNumerator.get();
			double cos =  numerator / (denominators1.get(0) * denominator2);
			if (cos != 0) {
				// cos就是结果矩阵中的某个元素，坐标 行：userId 列：userId2_score（右侧矩阵已经被转置）
				outKey.set(userId);
				outValue.set(userId2 + "_" + df.format(cos));
				// 输出格式为 key:行 value:列_值
				try {
					context.write(outKey, outValue);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}
    
    
	/*    public static void putKV(String user, String goods, String score) {
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
