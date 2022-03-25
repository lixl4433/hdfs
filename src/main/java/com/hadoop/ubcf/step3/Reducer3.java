package com.hadoop.ubcf.step3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
/**
 * 将评分矩阵转置
 */
public class Reducer3 extends Reducer<Text, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text();
	
	 //key:列号	 用户ID		value:行号_值,行号_值,行号_值,行号_值...	物品ID_分值
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		
		//text:行号_值		物品ID_分值
		for(Text text:values){  
            sb.append(text).append(",");
        }  
		String line = null;
		if(sb.toString().endsWith(",")){
			line = sb.substring(0, sb.length()-1);
		}
	
		
		outKey.set(key);
		outValue.set(line);
		
		context.write(outKey,outValue);  
	}
	
	
}