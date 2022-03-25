package com.hadoop.use.chineseword.sort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String str[] = value.toString().split("\t");
		context.write(new IntWritable(Integer.valueOf(str[1])), new Text(str[0]));
	}
}
