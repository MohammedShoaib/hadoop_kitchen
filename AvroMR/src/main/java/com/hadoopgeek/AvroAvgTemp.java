package com.hadoopgeek;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

public class AvroAvgTemp extends Configured implements Tool
{
	
	
	public static class AvroGenerateMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable>
	{
		
	}
	
	
	private void GenerateTempAvro(String src, String dest)
	{
		
	}
	
	public int run(String[] args) throws Exception
	{
		
		return 0;
	}
	
	public static void main(String[] args) 
	{
		
	}
}
