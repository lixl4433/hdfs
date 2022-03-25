package com.hadoop.ubcf.step7;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Joiner;
import com.hadoop.ubcf.job.ReadProperties;

	/**
	 *基于用户->文档  的协同过滤结果排序</br>
	 *排序同一用户文档分值由高到低的前10个
	 */
	public class Mapper7 extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text outKey = new Text();
		private Text outValue = new Text();
		private static final int order_num = Integer.parseInt(ReadProperties.get("com.hadoop.hdfs.order.num"));
		
		@Override
			protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
			
		       String[] rowAndLine_matrix1 = value.toString().split("\t");
		       //用户
		       String item_matrix1 = rowAndLine_matrix1[0];
		       //列_值
		       LinkedList<String> order_ = order(rowAndLine_matrix1[1]);
		       String order = Joiner.on(",").join(order_);
		       outKey.set(item_matrix1);
			   outValue.set(order);
			   //输出格式为	key:行	value:列_值
			   context.write(outKey, outValue);
			}
		
		
		public static LinkedList<String> order(String id_scores) {
			LinkedList<String> order = new LinkedList<>();
			Arrays.asList(id_scores.split(",")).forEach(id_score ->{
				int index = getIndex(order, Double.parseDouble(id_score.split("_")[1]));
				if(index > -1) order.add(index, id_score);
				if(order.size() > order_num) order.removeLast();
			});
			return order;
		}
		
		public static int getIndex(LinkedList<String> id_scores, double score) {
			for (int i = 0; i < id_scores.size(); i++)
				if(score > Double.parseDouble(id_scores.get(i).split("_")[1])) 
					return i;
			if(id_scores.size() < order_num)
				return id_scores.size();
			else
				return -1;
		}
	}

