package com.hadoop.use.chineseword;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.hadoop.use.chineseword.sort.DescComparator;
import com.hadoop.use.chineseword.sort.SortMapper;
import com.hadoop.use.chineseword.sort.SortReducer;

public class ChineseWordCount {
	
	 public static void main(String[] args) throws Exception {
			args = new String[4];
			args[0] = "hdfs://192.168.0.203:9000/test/星空.txt";
			args[1] = "hdfs://192.168.0.203:9000/test/星空/";
			Configuration conf = new Configuration();
			Job job = new Job(conf, "word count");
			job.setJarByClass(ChineseWordCount.class);
			job.setMapperClass(ChineseWordMapper.class);
			job.setCombinerClass(ChineseWordReducer.class);
			job.setReducerClass(ChineseWordReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);
	        
			args[2] = "hdfs://192.168.0.203:9000/test/星空/part-r-00000";
			args[3] = "hdfs://192.168.0.203:9000/test/星空/1/";
	        
        	Configuration conf2 = new Configuration();
        	Job job2 = new Job(conf2,"word sort");
        	job2.setJarByClass(ChineseWordCount.class);
        	job2.setMapperClass(SortMapper.class);
        	job2.setReducerClass(SortReducer.class);
        	job2.setSortComparatorClass(DescComparator.class);
        	job2.setMapOutputKeyClass(IntWritable.class);
        	job2.setMapOutputValueClass(Text.class);
        	
        	job2.setOutputKeyClass(Text.class);
        	job2.setOutputValueClass(IntWritable.class);
        	
        	FileInputFormat.addInputPath(job2, new Path(args[2]));
        	FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        	job2.waitForCompletion(true);
        	System.out.println("----------------");
	    }
}
