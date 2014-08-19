package com.hadoopgeek.matrix;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MatrixMapper extends Mapper<LongWritable, Text, Text, Text>
{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException 
	{
		// input format is ["a", 0, 0, 63] matrix name, i, j, value
		
		String[] csv = value.toString().split(",");
		String matrix = csv[0].trim();
		int row = Integer.parseInt(csv[1].trim());
		int col = Integer.parseInt(csv[2].trim());
				
		if(matrix.contains("a"))
		{
			for (int i=0; i < lMax; i++)
			{
				String akey =  Integer.toString(row) + "," + Integer.toString(i);
				context.write(new Text(akey), value);
		
			}
			
		}
		if(matrix.contains("b"))
		{
			for (int i=0; i < iMax; i++)
			{
				String akey = Integer.toString(i) + "," + Integer.toString(col);
				context.write(new Text(akey), value);
		
			}
			
		}
		
				
	}
	
	public void SetDimensions(int[] dimension)
	{
		iMax = dimension[0];
		jMax = dimension[1];
		kMax = dimension[2];
		lMax = dimension[3];
	}
	
	// matrix "a" dimension
	int iMax = 5;
	int jMax = 5;
	
	// matrix "b" dimension
	int kMax = 5;
	int lMax = 5;
	
}
