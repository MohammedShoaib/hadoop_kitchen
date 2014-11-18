package com.hadoopgeek.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Weather_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>
{

	private static final int MISSING = 9999;
	/**
	 * Map function, will get caller per line of input record
	 */
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException
	{
	
		try
		{
			String line = value.toString();
			String year = line.substring(15, 19);
			if (year.isEmpty()) return; // if no year then return
			
			String sTemp;
			if(line.charAt(87) == '+')
			{
				sTemp = line.substring(88,92);
			}
			else
			{
				sTemp = line.substring(87,92); //-ve sign too
			}
			
			if(sTemp.isEmpty()) return; // no temperature available
			
			int temp = Integer.parseInt(sTemp);
			
			String sQuality = line.substring(92,93);
			if(temp != MISSING && sQuality.matches("[01459]"))
			{
				
				System.out.println(year + "<->" + temp);
				context.write(new Text(year),new IntWritable(temp));
			}
			else
			{
				context.setStatus("Error detected !");
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
	}
	
}
