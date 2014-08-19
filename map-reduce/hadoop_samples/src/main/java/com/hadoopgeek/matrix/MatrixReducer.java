package com.hadoopgeek.matrix;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixReducer extends Reducer<Text,Text, Text, IntWritable> 
{

	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException 
	{
		
		int []a  = new int[5];
		int []b  = new int[5];
		//b, 2, 0, 30
		for (Text value : values)
		{
			System.out.println(value);
			String cell[] = value.toString().split(","); 
			if (cell[0].contains("a")) // take rows here
			{
				int col = Integer.parseInt(cell[2].trim());
				a[col] = Integer.parseInt(cell[3].trim());
			}
			else if (cell[0].contains("b")) // take col here
			{
				int row = Integer.parseInt(cell[1].trim());
				b[row] = Integer.parseInt(cell[3].trim());
			}
			
		}
		
		int total = 0;
		for (int i = 0; i < 5; i++)
		{
			int val = a[i] * b[i];
			total += val;
		}
		context.write(key, new IntWritable(total));
	}
	
}