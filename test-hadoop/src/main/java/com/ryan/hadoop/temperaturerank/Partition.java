package com.ryan.hadoop.temperaturerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partition extends Partitioner<KeyPair, Text> {

	@Override
	public int getPartition(KeyPair key, Text value, int num) {
		return key.getYear() % num;
	}
}