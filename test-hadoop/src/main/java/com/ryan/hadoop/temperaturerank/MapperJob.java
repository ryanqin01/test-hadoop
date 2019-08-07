package com.ryan.hadoop.temperaturerank;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperJob extends Mapper<LongWritable, Text, KeyPair, Text> {

	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] ss = line.split("\t");

		if (ss.length == 2) {
			try {
				Date date = sdf.parse(ss[0]);
				Calendar c = Calendar.getInstance();
				c.setTime(date);
				int year = c.get(1);
				String t = ss[1].substring(0, ss[1].indexOf("C"));
				KeyPair k = new KeyPair();
				k.setYear(year);
				k.setTemperature(Integer.parseInt(t));
				Text t1 = new Text(t);
				context.write(k, t1);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
}