package com.hadoop.use.word;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) {
        int count = 0;
 
        for (LongWritable value : values) {
            count += value.get();
        }
 
        try {
            context.write(key, new LongWritable(count));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}