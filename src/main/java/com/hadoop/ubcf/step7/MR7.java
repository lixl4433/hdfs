package com.hadoop.ubcf.step7;

import java.io.IOException;

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
import com.hadoop.ubcf.step6.Mapper6;

public class MR7 {
	private static String inputPath = ReadProperties.get("com.hadoop.hdfs.step4.output.file.path");
	private static String outputPath = ReadProperties.get("com.hadoop.hdfs.step7.output.file.path");

	public int run() {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", HDFSUtils.HDFS_URL);
			Job job = Job.getInstance(conf, "step7");
			// 如果未开启,使用 FileSystem.enableSymlinks()方法来开启符号连接。
			FileSystem.enableSymlinks();
			// 要使用符号连接，需要检查是否启用了符号连接
			boolean areSymlinksEnabled = FileSystem.areSymlinksEnabled();

			// 配置任务map和reduce类
			job.setJarByClass(MR7.class);
			
			MultithreadedMapper.setMapperClass(job, Mapper7.class);
			MultithreadedMapper.setNumberOfThreads(job, 10);

			job.setMapperClass(MultithreadedMapper.class);
			job.setReducerClass(Reducer7.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileSystem fs = FileSystem.get(conf);
			Path inpath = new Path(inputPath);
			if (fs.exists(inpath)) {
				FileInputFormat.addInputPath(job,inpath);
			} else {
				System.out.println(inpath);
				System.out.println("不存在");
			}

			Path outpath = new Path(outputPath);
			fs.delete(outpath, true);
			FileOutputFormat.setOutputPath(job, outpath);

			return job.waitForCompletion(true) ? 1 : -1;
		} catch (ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException  { 
		int result = -1;
		result = new MR7().run();
		if(result==1){
			System.out.println("step6运行成功");
		}else if(result==-1){
			System.out.println("step6运行失败");
		}
	  }
}
