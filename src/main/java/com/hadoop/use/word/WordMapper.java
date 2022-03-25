package com.hadoop.use.word;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	private final LongWritable one = new LongWritable(1);
	private Text word = new Text();
	public void map(LongWritable key, Text value, Context context) {
		/*Arrays.asList(value.toString().split("\t")).forEach(p ->{
				try {
					word.set(p);
					context.write(word, one);
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
		});*/
		for (String p : value.toString().split("\t")) {
			try {
				word.set(p);
				context.write(word, one);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
