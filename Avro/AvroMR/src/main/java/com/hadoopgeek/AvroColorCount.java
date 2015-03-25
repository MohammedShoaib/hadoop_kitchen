package com.hadoopgeek;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroColorCount extends Configured implements Tool
{
	private static final Schema SCHEMA = new Schema.Parser().parse(
			  "{" +
			  " \"type\": \"record\"," +
			  " \"name\": \"sample\"," +
			  " \"fields\": [" +
			  "            {\"name\": \"name\", \"type\": \"string\"}," +
			  "            {\"name\": \"desc\", \"type\": [\"null\", \"string\"]}," +
			  "            {\"name\": \"color\", \"type\": [\"null\", \"string\"]}" +
			  "             ]" +
			  "}"
			  );
			
			
	
	
	
	private void GenerateAvroDataGeneric (String dest) throws IOException
	{
		//load schema from resource.
		
		Schema.Parser parser = new Schema.Parser();
    	Schema schema = parser.parse(this.getClass().getClassLoader().getResourceAsStream("user.avsc"));
    	
    	//create some users
    	GenericRecord user1 = new GenericData.Record(schema);
    	user1.put("name", "Alyssa");
    	user1.put("favorite_number", 256);
    	// Leave favorite color null

    	GenericRecord user2 = new GenericData.Record(schema);
    	user2.put("name", "Ben");
    	user2.put("favorite_number", 7);
    	user2.put("favorite_color", "red");
    	
    	//serialize
    	String fname = dest + "/" + "gusers.avro";
    	Path fPath = new Path(fname);
    	FileSystem fs = FileSystem.get(getConf());
    	if (fs.exists(fPath))
    	{
    		fs.delete(fPath, true);
    	}
    	FSDataOutputStream fos = fs.create(fPath);
       	
    	DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    	DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    	dataFileWriter.create(schema, fos);
    	dataFileWriter.append(user1);
    	dataFileWriter.append(user2);
    	dataFileWriter.close();
    	
    	// Deserialize users from disk
    	SeekableInput input = new FsInput(fPath, getConf());
    	DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    	DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(input, datumReader);
    	GenericRecord user = null;
    	while (dataFileReader.hasNext()) {
    	// Reuse user object by passing it to next(). This saves us from
    	// allocating and garbage collecting many objects for files with
    	// many items.
    	user = dataFileReader.next(user);
    	System.out.println(user);
    	}
    	
    	fs.close();
	}
	
	private void GenerateAvroData(String dest) throws IOException
	{
		User user1 = new User();
		user1.setName("Jithesh");
		user1.setFavoriteNumber(9);
		user1.setFavoriteColor("Blue");
		
		User user2 = new User("Malini", 11, "Green");
		
		// Construct via builder
		User user3 = User.newBuilder()
		             .setName("Ganga")
		             .setFavoriteColor("Pink")
		             .setFavoriteNumber(null)
		             .build();
		
		User user4 = new User("Neel", 1, "Blue");
		User user5 = new User("Chand", 8, "Blue");
		User user6 = new User("Indira", 9, "Yellow");
		User user7 = new User("Mayoo", 6, "Pink");
        
		
		//Now let's serialize our Users to disk.
		
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
		String filename = dest + "/" + "users.avro";
		FileSystem fs = FileSystem.get(getConf());
		
		Path fPath = new Path(filename);
		
		if(fs.exists(fPath))
		{
			fs.delete(fPath, true);
		}
		
		FSDataOutputStream fso = fs.create(fPath);
		
		dataFileWriter.create(user1.getSchema(), fso);
		dataFileWriter.append(user1);
		dataFileWriter.append(user2);
		dataFileWriter.append(user3);
		dataFileWriter.append(user4);
		dataFileWriter.append(user5);
		dataFileWriter.append(user6);
		dataFileWriter.append(user7);
		
		dataFileWriter.close();
		
		// Deserialize Users from disk
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
		SeekableInput input = new FsInput(fPath, getConf());

		DataFileReader<User> dataFileReader = new DataFileReader<User>(input, userDatumReader);
		User user = null;
		while (dataFileReader.hasNext()) 
		{
		// Reuse user object by passing it to next(). This saves us from
		// allocating and garbage collecting many objects for files with
		// many items.
		user = dataFileReader.next(user);
		System.out.println(user);
		}
    	   	    	
    	dataFileReader.close();
	}
	
	
	//Mapper
	
	public static class AvroColMapper extends Mapper<AvroKey<User>, NullWritable, Text, IntWritable> 
	{

		@Override
		protected void map(
				AvroKey<User> user,
				NullWritable value,
				Mapper<AvroKey<User>, NullWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException 
		{
			CharSequence color = user.datum().getFavoriteColor();
			if (!color.equals(null))
			{
				context.write(new Text(color.toString()), new IntWritable(1));
			}
		}
		
	}
	
	// Reducer
	
	public static class AvroColReducer extends Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>>
	{

		@Override
		protected void reduce(
				Text key,
				Iterable<IntWritable> values,
				Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>>.Context context)
				throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable value : values)
			{
				sum+= value.get();
			}
			
			context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Integer>(sum));
			
		}
		
	}
	

	public int run(String[] args) throws Exception 
	{
		//GenerateAvroData("datasets/ccount");
		//GenerateAvroDataGeneric("datasets/ccount");
		
			if (args.length != 2) 
			{
				System.err.println("Usage: MapReduceColorCount <input path> <output path>");
				return -1;
		     }
			
		    Job job = Job.getInstance(getConf());
		    job.setJarByClass(AvroColorCount.class);
		    job.setJobName("Color Count");

		    Path input = new Path(args[0]);
			Path output = new Path(args[1]);
			
			FileSystem fs = FileSystem.get(getConf());
			
			if(!fs.exists(input)) 
			{
				System.err.println("Input file doesn't exists");
				return -1;
			}
			if(fs.exists(output)) 
			{
				fs.delete(output, true);
				System.err.println("Output file deleted");
			}
			fs.close();
			
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, output);

		    job.setInputFormatClass(AvroKeyInputFormat.class);
		    job.setMapperClass(AvroColMapper.class);
		    
		    AvroJob.setInputKeySchema(job, User.getClassSchema());
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);

		    job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		    job.setReducerClass(AvroColReducer.class);
		    
		    AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		    AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

		    System.out.println("MR Job Started !");
		    return (job.waitForCompletion(true) ? 0 : 1);
		    
		   
		
	}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration(); //switch to hdfs, should be removed when not running from eclipse
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		int res = ToolRunner.run(conf, new AvroColorCount(), args);
		 System.out.println("MR Job Completed !");
		System.exit(res);
	}
	

}
