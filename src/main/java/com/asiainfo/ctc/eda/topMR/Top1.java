package com.asiainfo.ctc.eda.topMR;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//求Top1程序
public class Top1 {
	public static final String INPUT_PATH = "hdfs://localhost:9000/kinyi/data/topN_data.txt";
	public static final String OUTPUT_PATH = "hdfs://localhost:9000/output/top1";

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
			if (fileSystem.exists(new Path(OUTPUT_PATH))) {
				fileSystem.delete(new Path(OUTPUT_PATH), true);
			}
			
			conf.set("word", "null");
			conf.set("num", "0");
			
			Job job = Job.getInstance(conf, Top1.class.getSimpleName());
			job.setJarByClass(Top1.class);
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
			FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		Text k2 = new Text();
		LongWritable v2 = new LongWritable();
		
		@Override
		protected void map(LongWritable k1, Text v1, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = v1.toString().split("\\t");
			for (String word : split) {
				k2.set(word);
				v2.set(1L);
				context.write(k2,v2);
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		LongWritable v3 = new LongWritable();
		String word;
		long num;
		
		@Override
		protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			word = conf.get("word");
			num = Long.parseLong(conf.get("num"));
		}
		
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s, Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable v2 : v2s) {
				sum += v2.get();
			}
			if (sum > num) {
				word = k2.toString();
				num = sum;
			}
		}
		
		@Override
		protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new Text(word), new LongWritable(num));
		}
	}
}