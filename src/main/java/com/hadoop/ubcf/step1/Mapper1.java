package com.hadoop.ubcf.step1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
/**
 * 根据用户行为列表得到用户、物品的评分矩阵
 */
public class Mapper1  extends Mapper<LongWritable, Text, Text, Text>  {
	private Text outKey = new Text();
	private Text outValue = new Text();
	/**
	 * key:行号1
	 * value:A,1,1	用户A对物品1有过操作(分值1)
	 * */
    @Override  
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
       String[] values = value.toString().split(",");
       String userID = values[0];
       String itemID = values[1];
       String score = values[2];
       
      
    	   
	   //key:列号	用户ID	value:行号_值	物品ID_分值
	   outKey.set(userID);
	   outValue.set(itemID+"_"+score);
	   
	   context.write(outKey, outValue);
       
    } 
}
