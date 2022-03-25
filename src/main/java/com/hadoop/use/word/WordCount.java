package com.hadoop.use.word;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void run(String[] args) {
		try {
			Configuration configuration = new Configuration();
			Job job = Job.getInstance(configuration, "word_count");
			// job.setUser("root");
			job.setJarByClass(WordCount.class);

			job.setMapperClass(WordMapper.class);
			job.setReducerClass(WordReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			job.setOutputKeyClass(Text.class);

			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			System.out.println(job.waitForCompletion(true) ? "运行成功" : "运行失败");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		args = new String[2];
		args[0] = "hdfs://192.168.0.203:9000/test/星空.txt";
		args[1] = "hdfs://192.168.0.203:9000/test/星空/";

		try {
			FileSystem.get(new Configuration()).deleteOnExit(new Path(args[1]));
			WordCount.run(args);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	
	
	
	
	
	
}