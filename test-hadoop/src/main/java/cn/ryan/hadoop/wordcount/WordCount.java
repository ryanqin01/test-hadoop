package cn.ryan.hadoop.wordcount;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("input/wordcount"));
		SimpleDateFormat sdf = new SimpleDateFormat();
		sdf.applyPattern("yyyy-MM-dd-HH-mm-ss");
		String path = sdf.format(new Date());
		FileOutputFormat.setOutputPath(job, new Path("output/wordcount_" + path));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
