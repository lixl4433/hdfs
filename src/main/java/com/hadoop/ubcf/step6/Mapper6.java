package com.hadoop.ubcf.step6;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoop.hdfs.HDFSUtils;
 
/**
 * 根据评分矩阵，将步骤4的输出中，用户已经有过行为的商品评分置零
 */
public class Mapper6 extends Mapper<LongWritable, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text();
	private List<String> cacheList = new ArrayList<String>();
			
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
		//key:行号 物品ID		value:列号_值,列号_值,列号_值,列号_值,列号_值...    用户ID_分值
		String line = null;
		while((line=br.readLine())!=null){
			cacheList.add(line);
		}
		br.close();
	}
 
 
	/**
	 * key: 行号	物品ID
	 * value:行	列_值,列_值,列_值,列_值	用户ID_分值
	 * */
    @Override  
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
       
       String[] rowAndLine_matrix1 = value.toString().split("\t");
       
       //矩阵行号	物品ID
       String item_matrix1 = rowAndLine_matrix1[0];
       //列_值
       String[] user_score_array_matrix1 = rowAndLine_matrix1[1].split(",");
       
       for(String line:cacheList){
    	   
    	   String[] rowAndLine_matrix2 = line.toString().split("\t");
    	   //右侧矩阵line
    	   //格式: 列 tab 行_值,行_值,行_值,行_值
    	   String item__matrix2 = rowAndLine_matrix2[0];
    	   String[] user_score_array_matrix2 = rowAndLine_matrix2[1].split(",");
    	   
    	   
    	   
	       //矩阵两位相乘得到的结果	
		   //double result = 0;
    	   
    	   //如果物品ID物品相同
    	   if(item_matrix1.equals(item__matrix2)){
    		   
    		   //遍历matrix1的列
    		   for(String user_score_matrix1:user_score_array_matrix1){
    			   boolean flag = false;
    			   String user_matrix1 = user_score_matrix1.split("_")[0];
    			   String score_matrix1 = user_score_matrix1.split("_")[1];
 
    			   //遍历matrix2的列
    			   for(String user_score_matrix2:user_score_array_matrix2){
        			   String user_matrix2 = user_score_matrix2.split("_")[0];
        			   if(user_matrix1.equals(user_matrix2)){
        				   flag = true;
        			   }
    			   }
    			   //该用户没有对该物品产生行为
    			   if(flag==false){
    				   outKey.set(item_matrix1);
    				   outValue.set(user_matrix1+"_"+score_matrix1);
    				   //输出格式为	key:行	value:列_值
    				   context.write(outKey, outValue);
    			   }
    		   }
    	   }
    	  
       }
    } 
}
