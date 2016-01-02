package com.asiainfo.ctc.eda.topMR;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//求TopN程序2
public class TopN2 {
	public static final String INPUT_PATH = "hdfs://localhost:9000/output/topN/part-r-00000";
	public static final String OUTPUT_PATH = "hdfs://localhost:9000/output/topN2";

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
			if (fileSystem.exists(new Path(OUTPUT_PATH))) {
				fileSystem.delete(new Path(OUTPUT_PATH), true);
			}

			conf.set("N", args[0]);

			Job job = Job.getInstance(conf, TopN2.class.getSimpleName());
			job.setJarByClass(TopN2.class);
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);
			job.setMapOutputKeyClass(NewK2.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
			FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class MyMapper extends Mapper<LongWritable, Text, NewK2, NullWritable> {
		@Override
		protected void map(LongWritable k1, Text v1, Mapper<LongWritable, Text, NewK2, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = v1.toString().split("\\t");
			NewK2 k2 = new NewK2(split[0], Long.parseLong(split[1]));
			context.write(k2, NullWritable.get());
		}
	}

	public static class MyReducer extends Reducer<NewK2, NullWritable, Text, LongWritable> {
		long topN_num;

		@Override
		protected void setup(Reducer<NewK2, NullWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String topN = conf.get("N");
			topN_num = Long.parseLong(topN);
		}

		@SuppressWarnings("unused")
		@Override
		protected void reduce(NewK2 k2, Iterable<NullWritable> v2s,
				Reducer<NewK2, NullWritable, Text, LongWritable>.Context context)
						throws IOException, InterruptedException {
			// if (topN_num > 0) { //写在这的效果:次数相同的单词全列出来
			for (NullWritable nullWritable : v2s) {
				if (topN_num > 0) {//写在这的效果:次数相同的也只会列出限定个数
					context.write(new Text(k2.key), new LongWritable(k2.value));
					topN_num--;
				}
			}
		}
	}

	public static class NewK2 implements WritableComparable<NewK2> {
		String key;
		long value;

		public NewK2() {
			super();
		}

		public NewK2(String key, long value) {
			super();
			this.key = key;
			this.value = value;
		}

		public int compareTo(NewK2 o) {
			return (int) -(this.value - o.value);
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(key);
			out.writeLong(value);
		}

		public void readFields(DataInput in) throws IOException {
			this.key = in.readUTF();
			this.value = in.readLong();
		}

		@Override
		public String toString() {
			return "NewK2 [key=" + key + ", value=" + value + "]";
		}

	}
}