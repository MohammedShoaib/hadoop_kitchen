package com.hadoopgeek.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Weather extends Configured implements Tool
{
	

	public int run(String[] args) throws Exception
	{
		if(args.length < 2)
		{
			System.err.println("Usage : Weather <input path> <output path>");
			System.exit(-1);
		}
		
		
		Job job = Job.getInstance(getConf());
		//conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020"); // take this value from core-site.xml
		FileSystem fs = FileSystem.get(getConf());
			
		
		job.setJarByClass(Weather.class);
				
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
				
		job.setMapperClass(Weather_Mapper.class);
		job.setReducerClass(Weather_Reducer.class);
				
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//job.setNumReduceTasks(2);
				
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
				
		int ret = job.waitForCompletion(true) ? 0 : 1;
				
		System.out.println("MR Job Completed !");
		
		return ret;
	}
	
	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new Weather(), args);
        System.exit(res);
	}
}
