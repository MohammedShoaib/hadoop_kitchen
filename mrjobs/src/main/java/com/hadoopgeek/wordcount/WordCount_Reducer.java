package com.hadoopgeek.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount_Reducer extends Reducer<Text,IntWritable, Text, IntWritable> 
{

	/**
	 * @param key Input key from the mapper
	 * @param values number of times key occurs
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
					Context context)
					throws IOException, InterruptedException 
	{
		int total = 0;
		for (IntWritable value:values)
		{
			total+=1;
		}
		
		context.write(key, new IntWritable(total));
	
	}

}
