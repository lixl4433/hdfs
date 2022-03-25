package com.hadoop.ubcf.step1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
/**
 * 
 * 
 * 根据用户行为列表得到用户、物品的评分矩阵
 */
public class Reducer1 extends Reducer<Text, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text();
	
	 ///key:列号	用户ID	value:行号_值	物品ID_分值
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String itemID=key.toString();
		
		//userID,score
		Map<String,Integer> map = new HashMap<>();
		
		//text:行号_值
		for(Text value:values){  
			String[] split = value.toString().split("_");
			String userID = split[0];
			String score = split[1];
			
			if(map.get(userID)==null){
				map.put(userID, Integer.parseInt(score));
			}else{
				Integer preScore = map.get(userID);
				map.put(userID, preScore+Integer.parseInt(score));
			}
		}
		StringBuilder sb =  new StringBuilder();
		for(Map.Entry<String,Integer> entry:map.entrySet()){
			String userID = entry.getKey();
			String score = String.valueOf(entry.getValue());
			sb.append(userID).append("_").append(score).append(",");
		}
		String line = null;
		if(sb.toString().endsWith(",")){
			line = sb.substring(0, sb.length()-1);
		}
	
		//key:行号 物品ID		value:列号_值,列号_值,列号_值,列号_值,列号_值...    用户ID_分值
		outKey.set(itemID);
		outValue.set(line);
		
		context.write(outKey,outValue);  
	}
	
	
}
