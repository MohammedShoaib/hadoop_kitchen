package com.hadoopgeek.matrix;

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

public class Matrix 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        
    	Path inputPath = new Path(args[0]);
    	Path outputPath = new Path(args[1]);
    	
    	Configuration conf = new Configuration(true);
    	
    	// Create Job
    	Job job = new Job(conf, "Matrix");
    	job.setJarByClass(Matrix.class);
    	
    	//Set up MapReduce
    	job.setMapperClass(MatrixMapper.class);
    	job.setReducerClass(MatrixReducer.class);
    	//job.setNumReduceTasks(1);
    	
    	//specify key/value
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	
    	//Input
    	FileInputFormat.addInputPath(job, inputPath);
    	job.setInputFormatClass(TextInputFormat.class);
    	
    	//Output
    	
    	FileOutputFormat.setOutputPath(job, outputPath);
    	job.setOutputFormatClass(TextOutputFormat.class);
    	
    	// Delete OutputFile if exists
    	FileSystem hdfs = FileSystem.get(conf);
    	if(hdfs.exists(outputPath))
    		hdfs.delete(outputPath,true);
    	
    	//Execute job
    	int code = job.waitForCompletion(true) ? 0 : 1;
    	System.exit(code);
    	
    	
    	
    }
}
