package com.hadoop.ubcf.step5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.hdfs.HDFSUtils;
import com.hadoop.ubcf.job.ReadProperties;
import com.hadoop.ubcf.step4.Mapper4;

public class MR5 {

	private static String inputPath = ReadProperties.get("com.hadoop.hdfs.step4.output.file.path");
	private static String outputPath = ReadProperties.get("com.hadoop.hdfs.step5.output.file.path");

	public int run() {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", HDFSUtils.HDFS_URL);

			Job job = Job.getInstance(conf, "step5");
			
			job.setJarByClass(MR5.class);
			
			MultithreadedMapper.setMapperClass(job, Mapper5.class);
			MultithreadedMapper.setNumberOfThreads(job, 10);

			job.setMapperClass(MultithreadedMapper.class);
			job.setReducerClass(Reduce5.class);
			job.setNumReduceTasks(1);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			FileSystem.get(conf).delete(new Path(outputPath), true);// 调用任务前先删除输出目录
			return job.waitForCompletion(true) ? 1 : -1;
		} catch (Exception e) {
			
		}
		return -1;

	}
}