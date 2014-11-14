package com.hadoopgeek;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class AvroWordCount extends Configured implements Tool
{
	
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		
		Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException 
		{
			
			String line = value.toString();
			StringTokenizer st = new StringTokenizer(line);
			while(st.hasMoreTokens())
			{
				word.set(st.nextToken());
				context.write(word, new IntWritable(1));
			}
			
		}
		
	}
	
	public static class WordCountReducer extends Reducer<Text, IntWritable ,
	                             AvroWrapper<Pair< CharSequence, Integer >>, NullWritable>
	{

		@Override
		protected void reduce(
				Text key,
				Iterable<IntWritable> values,
				Reducer<Text, IntWritable, AvroWrapper<Pair<CharSequence, Integer>>, NullWritable>.Context context)
				throws IOException, InterruptedException
		{
			    int sum = 0;
				for(IntWritable value : values)
				{
					sum += value.get();
				}
				context.write(new AvroWrapper<Pair<CharSequence,Integer>>
				   (new Pair<CharSequence,Integer>(key.toString(),sum)), NullWritable.get());
		}
		
		
		
	}

	public int run(String[] args) throws Exception 
	{
		if (args.length != 2) 
		{
		      System.err.println("Usage: AvroWordCount <input path> <output path>");
		      return -1;
	    }
		
		
		
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(AvroWordCount.class);
		job.setJobName("avro-wordcount");
		
		
		AvroJob.setOutputKeySchema(job, Pair.getPairSchema(Schema.create(Type.STRING),
					        Schema.create(Type.INT)));
		
		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		FileSystem fs = FileSystem.get(getConf());
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
				
		if(!fs.exists(input)) 
		{
			System.err.println("Input file doesn't exists");
			return -1;
		}
		if(fs.exists(output)) 
		{
			fs.delete(output, true);
			System.err.println("Output file deleted");
		}
		fs.close();
				
		FileInputFormat.addInputPath(job, input);
		
		FileOutputFormat.setOutputPath(job, output);
				
		job.waitForCompletion(true);
				
		System.out.println("MR Job Completed !");
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		int res = ToolRunner.run(conf, new AvroWordCount(), args);
		System.exit(res);
	}

}
