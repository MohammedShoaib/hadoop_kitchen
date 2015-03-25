package com.hadoopgeek.SerDe;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;


/**
 * Hello world!
 *
 */
public class SerDeTest 
{
	public static void main(String[] args) throws IOException {

		//testWritables();
		testTextPair();

	}
    
	public static void testWritables() throws IOException {
		
		byte[] bytes = serialize(15);
		System.out.println(StringUtils.byteToHexString(bytes));

		byte[] vbytes = vSerialize(15);
		System.out.println(StringUtils.byteToHexString(vbytes));

		int rValue = deSerialize(bytes);
		System.out.println(rValue);

		IntWritable a = new IntWritable(10);
		IntWritable b = new IntWritable(20);
		CompareObjects(a, b);

		int o1 = 10;
		int o2 = 20;

		CompareStreams(serialize(o1), serialize(o2));

	}
    
	public static void testTextPair() throws IOException {
		
		TextPair tp = new TextPair("first", "second");
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutput dOut = new DataOutputStream(out);
		tp.write(dOut);

		System.out.println(StringUtils.byteToHexString(out.toByteArray()));

		TextPair tp1 = new TextPair();
		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		DataInput dIn = new DataInputStream(in);
		tp1.readFields(dIn);
		System.out.println(tp1.toString());

		TextPair tp2 = new TextPair("second", "first");
		ByteArrayOutputStream out1 = new ByteArrayOutputStream();
		DataOutput dOut1 = new DataOutputStream(out1);
		tp2.write(dOut1);

		RawComparator<IntWritable> raw = WritableComparator.get(TextPair.class);
						
		int comp = raw.compare(out.toByteArray(), 0,
				out.toByteArray().length, out1.toByteArray(), 0,
				out1.toByteArray().length);
		
		
		System.out.println(comp);
	}

    public static byte[] serialize(int num) throws IOException
    {
    	IntWritable iWrite = new IntWritable(num);
    	ByteArrayOutputStream out = new ByteArrayOutputStream();
    	DataOutput dOut = new DataOutputStream(out);
    	iWrite.write(dOut);
    	return out.toByteArray();   
    	
    }
    
    public static int deSerialize(byte[] bytes) throws IOException
    {
    	ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    	DataInput dIn = new DataInputStream(in);
    	IntWritable iWrite = new IntWritable();
    	iWrite.readFields(dIn);
    	return iWrite.get();
        	
    }
    
    public static byte[] vSerialize(int num) throws IOException
    {
    	VIntWritable vWrite = new VIntWritable(num);
    	ByteArrayOutputStream out = new ByteArrayOutputStream();
    	DataOutput dOut = new DataOutputStream(out);
    	vWrite.write(dOut);
    	return out.toByteArray();   
    	
    }
    
    
    public static void CompareObjects(IntWritable a, IntWritable b)
    {
    	RawComparator<IntWritable> raw = WritableComparator.get(IntWritable.class);
    	if (raw.compare(a, b) == 0)
    	{
    		System.out.println("Same !");
    	}
    	else if(raw.compare(a, b) > 0)
    	{
    		System.out.println(a + " is larger");
    	}
    	else
    	{
    		System.out.println(b + " is larger");
    	}
    }
    
    public static void CompareStreams(byte[] a, byte[]b)
    {
    	RawComparator<IntWritable> raw = WritableComparator.get(IntWritable.class);
    	if(raw.compare(a, 0, a.length, b, 0, b.length) == 0)
    	{
    		System.out.println("Same !");
    	}
    	else if (raw.compare(a, 0, a.length, b, 0, b.length) > 0)
    	{
    		System.out.println(a + "is larger than " + b);
    	}
    	else
    	{
    		System.out.println(b + "is larger than " + a);
    	}
    } 
}
