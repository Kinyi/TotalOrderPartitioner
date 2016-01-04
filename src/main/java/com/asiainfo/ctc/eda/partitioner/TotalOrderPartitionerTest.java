package com.asiainfo.ctc.eda.partitioner;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * 没成功
 * @author Kinyi_Chan
 *
 */
public class TotalOrderPartitionerTest {

	static final String INPUT_PATH = "hdfs://localhost:9000/kinyi/data/twoCols.txt";
	static final String OUT_PATH = "hdfs://localhost:9000/output/";
	static final String URI = "hdfs://localhost:9000/";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		final Job job = Job.getInstance(conf, TotalOrderPartitionerTest.class.getSimpleName());
		final FileSystem fileSystem = FileSystem.get(new URI(URI),new Configuration());
		final Path path = new Path(OUT_PATH);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
		job.setJarByClass(TotalOrderPartitionerTest.class);
		// 1.1输入目录在哪里
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		// 指定对输入数据进行格式化处理的类
		job.setInputFormatClass(TextInputFormat.class);
		// 1.2指定自定义的mapper类
		job.setMapperClass(MyMapper.class);
		// 指定map输出的<k,v>类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		// 1.3分区
		job.setPartitionerClass(TotalOrderPartitioner.class);	
		
		RandomSampler<Text, LongWritable> sampler = new InputSampler.RandomSampler<Text, LongWritable>(0.1, 10000, 10);
		Path input = FileInputFormat.getInputPaths(job)[0];
		input = input.makeQualified(input.getFileSystem(conf));
		Path partitionFile = new Path(input, "_partitions");
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		InputSampler.writePartitionFile(job, sampler);
		
		job.setNumReduceTasks(4);
		// 1.4排序、分组
		// 1.5归约（可选）
		// 2.2指定自定义的reducer类
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		// 2.3指定输出的路径
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		// 指定输出的格式化类
		job.setOutputFormatClass(TextOutputFormat.class);
		// 把作业提交给jobTracker运行
		job.waitForCompletion(true);
	}

	static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		protected void map(LongWritable k1, Text v1, Context context)
				throws java.io.IOException, InterruptedException {

			final String line = v1.toString();

			final String[] splited = line.split("\t");
			context.write(new Text(splited[0]), new LongWritable(Long.parseLong(splited[1])));
		}
	}

	/**
	 * KEYIN 即k2 表示行中出现的单词
	 * VALUEIN 即v2 表示行中出现的单词的次数
	 * KEYOUT 即k3 表示文本中出现的不同单词
	 * VALUEOUT 即v3 表示文本中出现的不同单词的总次数
	 * 
	 */
	static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		protected void reduce(Text k2, java.lang.Iterable<LongWritable> v2s,Context context)//此处的Context不能加org.apache.hadoop.mapreduce.reducer.
				throws java.io.IOException, InterruptedException {//可能因为没有覆盖原来的reduce方法，没有进行累加
			for (LongWritable longWritable : v2s) {
				context.write(k2, longWritable);
			}
		}
	}
}