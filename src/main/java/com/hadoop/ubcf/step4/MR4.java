package com.hadoop.ubcf.step4;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
import com.hadoop.ubcf.step2.Mapper2;

/**
 *          用户与用户相似度矩阵X评分矩阵（经过步骤3转置）
 */
public class MR4 {
	private static String inputPath = ReadProperties.get("com.hadoop.hdfs.step2.output.file.path");
	private static String outputPath = ReadProperties.get("com.hadoop.hdfs.step4.output.file.path");
	// 将step1中输出的转置矩阵作为全局缓存
	private static String cache = ReadProperties.get("com.hadoop.hdfs.step3.output.file.path") + "/part-r-00000";

	public int run() {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", HDFSUtils.HDFS_URL);
			Job job = Job.getInstance(conf, "step4");
			// 如果未开启,使用 FileSystem.enableSymlinks()方法来开启符号连接。
			FileSystem.enableSymlinks();
			// 添加分布式缓存文件
			job.addCacheArchive(new URI(cache));

			// 配置任务map和reduce类
			job.setJarByClass(MR4.class);
			// job.setJar("F:\\eclipseworkspace\\UserCF\\UserCF.jar");

			MultithreadedMapper.setMapperClass(job, Mapper4.class);
			MultithreadedMapper.setNumberOfThreads(job, 100);

			job.setMapperClass(MultithreadedMapper.class);
			job.setReducerClass(Reducer4.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileSystem fs = FileSystem.get(conf);
			Path inpath = new Path(inputPath);
			if (fs.exists(inpath)) {
				FileInputFormat.addInputPath(job, inpath);
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
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return -1;
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		int result = -1;
		result = new MR4().run();
		if (result == 1) {
			System.out.println("step4运行成功");
		} else if (result == -1) {
			System.out.println("step4运行失败");
		}
	}
}