package com.hadoopgeek.weather;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Weather 
{
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		
		if(args.length < 2)
		{
			System.err.println("Usage : Weather <input path> <output path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020"); // take this value from core-site.xml
		FileSystem fs = FileSystem.get(conf);
			
		
		job.setJarByClass(Weather.class);
				
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
				
		job.setMapperClass(Weather_Mapper.class);
		job.setReducerClass(Weather_Reducer.class);
				
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(2);
				
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
				
		if(!fs.exists(input)) 
		{
			System.err.println("Input file doesn't exists");
			return;
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
		
	}
}
