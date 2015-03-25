package com.hadoopgeek.test;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Test extends Configured implements Tool
{

	public int run(String[] args) throws Exception 
	{
		PrintConfigurations();
		return 0;
	}
	
	public static void main(String[] args) throws Exception
	{
		System.out.println("Configurations");
		int exit = ToolRunner.run(new Test(),args);
		System.exit(exit);
		
	}
	
	public void PrintConfigurations()
	{
		Configuration conf = getConf();
		for (Entry<String, String> entry: conf) {
		System.out.printf("%s=%s\n", entry.getKey(), entry.getValue()); }
		
	}

}
