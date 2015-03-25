package com.hadoopgeek.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Weather_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>
{

	/**
	 * Reducer function.
	 */
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException
	{
		try 
		{
			int max = 0;
			
			for(IntWritable value : values)
			{
				if(value.get() > max)
				{
					max = value.get();
				}
			}
			System.out.println("Max Temp of Year " + key.toString() + " is " + max );
			context.write(key, new IntWritable(max));
		} 
		catch (Exception e) 
		{
			
		}
	}
	
}
