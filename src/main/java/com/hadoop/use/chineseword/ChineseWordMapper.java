package com.hadoop.use.chineseword;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class ChineseWordMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		byte[] bt = value.getBytes();
		InputStream ip = new ByteArrayInputStream(bt);
		Reader read = new InputStreamReader(ip);
		IKSegmenter iks = new IKSegmenter(read, true);
		Lexeme t;
		while ((t = iks.next()) != null) {
			word.set(t.getLexemeText());
			context.write(word, one);
		}
	}
}
