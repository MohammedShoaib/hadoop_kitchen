package com.hadoopgeek.wordcount;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map class which performs map computation.
 *  @author Jithesh Chandrasekharan
*/

public class WordCount_Mapper extends Mapper<LongWritable,Text, Text, IntWritable>
{

	/**
	 * @param key is the byte offset of line
	 * @param value is single line in the document
	 * @param context to write the output of computation
	 */
	//@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException 
	{
		String line = value.toString();
		String[] words = line.split(" ");
		for (String word : words)
		{
			context.write(new Text(word), new IntWritable(1));
		}
	}

}
