package com.ryan.hadoop.temperaturerank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJob extends Reducer<KeyPair, Text, IntWritable, Text> {

	protected void reduce(KeyPair key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		for (Text v : value) {
			context.write(new IntWritable(key.getYear()), v);
		}
	}

}