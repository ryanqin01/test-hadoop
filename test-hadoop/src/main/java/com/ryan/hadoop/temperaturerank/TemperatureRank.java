package com.ryan.hadoop.temperaturerank;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TemperatureRank {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "temperature rank");
		job.setJarByClass(TemperatureRank.class);
		job.setMapperClass(MapperJob.class);
		job.setReducerClass(ReducerJob.class);
		job.setMapOutputKeyClass(KeyPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(Partition.class);
		job.setSortComparatorClass(Sort.class);
		job.setGroupingComparatorClass(Group.class);
		job.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job, new Path("input/temperaturerank"));
		SimpleDateFormat sdf = new SimpleDateFormat();
		sdf.applyPattern("yyyy-MM-dd-HH-mm-ss");
		String path = sdf.format(new Date());
		FileOutputFormat.setOutputPath(job, new Path("output/temperaturerank_" + path));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
