package com.hadoopgeek.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;


public class WordCountTester
{
	 MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	 ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	 
	 @Before
	 public void setUp()
	 {
		 WordCount_Mapper wcMapper = new WordCount_Mapper();
		 WordCount_Reducer wcReducer = new WordCount_Reducer();
		 mapDriver = MapDriver.newMapDriver(wcMapper);
		 reduceDriver = ReduceDriver.newReduceDriver(wcReducer);
		 
	 }
	 
	 @Test
	  public void testMapper() throws IOException {
	    mapDriver.withInput(new LongWritable(), new Text(
	        "Jithesh"));
	    mapDriver.withOutput(new Text("Jithesh"), new IntWritable(1));
	    mapDriver.runTest();
	  }
	 
	 @Test
	  public void testReducer() throws IOException {
	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    reduceDriver.withInput(new Text("Jithesh"), values);
	    reduceDriver.withOutput(new Text("Jithesh"), new IntWritable(2));
	    reduceDriver.runTest();
	  }

}
