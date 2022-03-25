package com.hadoop.use.chineseword.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

public class DescComparator extends WritableComparator{

	protected DescComparator() {
		super(IntWritable.class,true);
	}

	@Override
	public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
			int arg4, int arg5) {
		return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);
	}
	@Override
	public int compare(Object a,Object b){
		return -super.compare(a, b);
	}
}
