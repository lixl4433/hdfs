package com.hadoop.ubcf.job;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

import com.hadoop.hdfs.HDFSUtils;
import com.hadoop.ubcf.step1.MR1;
import com.hadoop.ubcf.step2.MR2;
import com.hadoop.ubcf.step3.MR3;
import com.hadoop.ubcf.step4.MR4;
import com.hadoop.ubcf.step5.MR5;
import com.hadoop.ubcf.step7.MR7;

public class UserCFJob {

	public static void main(String[] args) {
		ReadProperties.init();
		HDFSUtils.init();
		/*		HDFSUtils.uploadFIleToHDFS(ReadProperties.get("com.hadoop.upload.file.path"),
						ReadProperties.get("com.hadoop.input.file.path"));
				System.out.println("加载原始矩阵到hdfs完毕");*/
		System.out.println(df.apply(new Date()));
		if (new MR1().run() == 1)
			System.out.println("step1运行成功");
		else
			System.out.println("step1运行失败");

		System.out.println(df.apply(new Date()));
		if (new MR2().run() == 1)
			System.out.println("step2运行成功");
		else
			System.out.println("step2运行失败");

		System.out.println(df.apply(new Date()));
		if (new MR3().run() == 1)
			System.out.println("step3运行成功");
		else
			System.out.println("step3运行失败");

		System.out.println(df.apply(new Date()));

		if (new MR4().run() == 1)
			System.out.println("step4运行成功");
		else
			System.out.println("step4运行失败");
		System.out.println(df.apply(new Date()));

		/*if (new MR5().run() == 1)
			System.out.println("step5运行成功");
		else
			System.out.println("step5运行失败");*/

		/*System.out.println(df.apply(new Date()));
		if (new MR6().run() == 1)
		System.out.println("step6运行成功");
		else
		System.out.println("step6运行失败");*/

		System.out.println(df.apply(new Date()));
		if (new MR7().run() == 1)
			System.out.println("step7运行成功");
		else
			System.out.println("step7运行失败");
		System.out.println(df.apply(new Date()));
	}

	public static Function<Date, String> df = (Date p1) -> {
		String dfs = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(dfs);
		return sdf.format(p1);
	};

}
