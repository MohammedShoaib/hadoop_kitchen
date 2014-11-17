package com.hadoopgeek;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Hello world!
 *
 */
public class SeqFileApp 
{
	private static final String[] DATA = { "One, two, buckle my shoe",
		"Three, four, shut the door",
		"Five, six, pick up sticks",
		"Seven, eight, lay them straight",
		"Nine, ten, a big fat hen" };
	
	
	public static void main( String[] args ) throws IOException
    {
    	String dest = "datasets/sequence/testseq";
    	//WriteSeqFile(dest);
    	ReadSeqFile(dest);
    	
    }
	
	public static void WriteSeqFile(String dest) throws IOException
	{
		Configuration conf = new Configuration();
    	conf.set("fs.defaultFS", "hdfs://localhost:9000"); // take this value from core-site.xml
    	Path path = new Path(dest);
    	
    	IntWritable key = new IntWritable();
    	Text value = new Text();
    	
    	Option filePath = SequenceFile.Writer.file(path);
    	Option keyClass = SequenceFile.Writer.keyClass(IntWritable.class);
    	Option valueClass = SequenceFile.Writer.valueClass(Text.class);
    	
    	Writer writer = null;
    	try
    	{
    		writer = SequenceFile.createWriter(conf, filePath, keyClass, valueClass);
    		for (int i = 0; i < 100; i++) {
    			key.set(100 - i);
    			value.set(DATA[i % DATA.length]); 
    			System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value); 
    			writer.append(key, value);
    		}
    	}
    	finally
    	{
    		IOUtils.closeStream(writer);
    	}
    	
    	System.out.println("Done Writing !");
	}
	
	public static void ReadSeqFile(String dest) throws IOException
	{
		Configuration conf = new Configuration();
    	conf.set("fs.defaultFS", "hdfs://localhost:9000"); // take this value from core-site.xml
    	Path path = new Path(dest);
	
		Reader reader = null;
    	Reader.Option fileRPath = SequenceFile.Reader.file(path);
    	
    	try
    	{
    		reader = new SequenceFile.Reader(conf, fileRPath);
    		Writable key1 = (Writable)
    				ReflectionUtils.newInstance(reader.getKeyClass(), conf); 
    		Writable value1 = (Writable)
    				ReflectionUtils.newInstance(reader.getValueClass(), conf); 
    		long position = reader.getPosition();
    		
    		while (reader.next(key1, value1)) 
    		{
    			String syncSeen = reader.syncSeen() ? "*" : "";
    			System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key1, value1); 
    			position = reader.getPosition(); // beginning of next record
    		}
    		
    		
    	}
    	finally
    	{
    		IOUtils.closeStream(reader);
    	}
	}
	
	
}
