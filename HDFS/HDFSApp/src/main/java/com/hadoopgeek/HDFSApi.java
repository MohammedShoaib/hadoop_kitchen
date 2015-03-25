package com.hadoopgeek;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

/**
 * A class to interact with  HDFS using Java
 * @author Jithesh Chandrasekharan
 * @version 1.0
 */
public class HDFSApi 
{
	public static final boolean DEV_MODE = false;
	/**
	 * Entry point of this application
	 * @param args commandline arguments
	 * @throws IOException 
	 */
    public static void main( String[] args ) throws IOException
    {
        System.out.println( "Hello World!" );
        //Compress("input/matrix.txt", "gz");
       // DeCompress("input/matrix.gz");
          WriteFile("/Users/jit/Downloads/temp/a.text", "hdfs://quickstart.cloudera:8020/user/cloudera/data");
        //ReadFile("/Users/jit/Downloads/a.text", "/Users/jit/Downloads/temp");
        //ReadFile("/user/cloudera/data/cm_ap.sh", "/Users/jit/Downloads/temp");
        //PrintFileStatus("input/ma.txt");
        
    }
    
    /**
     * 
     * @param src file to be compressed
     * @param type compression type to be used. Example : gz (gzip)
     * @return true/false
     * @throws IOException 
     */
	public static boolean Compress(String src,String type) throws IOException 
    {
		InputStream in = null;
		OutputStream out = null;
		CompressionOutputStream cout = null;
		
    	try
    	{
    		Configuration conf = new Configuration();
        	conf.set("fs.defaultFS", "hdfs://localhost:9000"); // take this value from core-site.xml
        	FileSystem fs = FileSystem.get(conf);
        	Path srcPath = new Path(src);
        	if(!fs.exists(srcPath))
        	{
        		System.err.println(src + " does not exist");
        		return false;
        	}
        	in = fs.open(srcPath);
        	
        	Path destPath = new Path(FilenameUtils.removeExtension(src) + "." + type);
        	if(fs.exists(destPath))
        	{
        		fs.delete(destPath, true);
        	}
       	
        	out = fs.create(destPath);
        	if(out == null || in == null)
        	{
        		return false;
        	}
        	CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        	CompressionCodec codec = factory.getCodec(destPath);
        	if(codec == null)
        	{
        		System.err.println("Could not find a codec for " + destPath);
        		return false;
        	}
        	cout = codec.createOutputStream(out);
        	IOUtils.copyBytes(in, cout, conf);
        	cout.finish();
        	
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    		return false;
    	}
    	finally
    	{
    		
				out.close();
				in.close();
				cout.close();
		
    	}
    	
    	return true;
    	
    }
	
	/**
	 * 
	 * @param src file to decompress
	 * @return true if succeeded else false
	 * @throws IOException 
	 */
	public static boolean DeCompress(String src) throws IOException
	{
		InputStream in = null;
		OutputStream out = null;
		CompressionInputStream cin = null;
		FileSystem fs = null;
		
		try
		{
			Configuration conf = new Configuration();
			
			conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020"); // take this value from core-site.xml/ Use it only when debugging.
			fs = FileSystem.get(conf);
        	Path srcPath = new Path(src);
        	if(!fs.exists(srcPath))
        	{
        		System.err.println(src + " does not exist");
        		return false;
        	}
        	CompressionCodecFactory factory = new CompressionCodecFactory(conf);
            CompressionCodec codec = factory.getCodec(srcPath);
            if(codec == null)
            {
            	return false;
            }
            String dest = CompressionCodecFactory.removeSuffix(src, codec.getDefaultExtension());
            Path destPath = new Path(dest);
            in = fs.open(srcPath);
            cin = codec.createInputStream(in);
            out = fs.create(destPath);
            IOUtils.copyBytes(cin, out, conf); 
       	
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			out.flush();
			out.close();
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
			fs.close();
			
		}
		
		return true;
	}
	
	/**
	 * This method will copy a file from local file system to HDFS
	 * @param src File to be written
	 * @param dest Location in HDFS in which file is copied
	 * @return success/fail
	 * @throws IOException
	 */
	
	public static boolean WriteFile(String src, String dest) throws IOException
	{
		
		FileSystem fs = null;
		InputStream in = null;
		OutputStream out = null;
		
		try
		{
			Configuration conf = new Configuration();
			//conf.set("fs.defaultFS", "hdfs://localhost:9000"); // take this value from core-site.xml/ Use it only when debugging.
			//conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020");
			fs = FileSystem.get(conf); // hdfs
			if(fs == null) return false;
			
			in = new BufferedInputStream(new FileInputStream(src));
						
			dest = dest + "/" + src.substring((src.lastIndexOf("/") + 1)); // merge file name
			
			Path destPath = new Path(dest);
			if(fs.exists(destPath))
			{
				fs.delete(destPath, true);
			}
			out = fs.create(destPath);
			if(out == null) return false;
			
			IOUtils.copyBytes(in, out, conf);
		}
		finally
		{
			in.close();
			out.close();
			fs.close();
		}
		return true;
	}
	
	/**
	 * This method will copy a file from HDFS to local file system
	 * @param src file in HDFS
	 * @param dest local file system location where file should be copied
	 * @return true/false
	 * @throws IOException
	 */
	public static boolean ReadFile(String src, String dest) throws IOException
	{

		FileSystem fs = null;
		InputStream in = null;
		OutputStream out = null;
		
		try
		{
			Configuration conf = new Configuration();
			//conf.set("fs.defaultFS", "hdfs://localhost:9000"); // take this value from core-site.xml/ Use it only when debugging.
			//conf.set("fs.defaultFS", "hdfs://172.16.195.128:8020");
			conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020");
			fs = FileSystem.get(conf);
			if(fs == null) return false;
			
			Path srcPath = new Path(src);
			
			if (!fs.exists(srcPath))
			{
				System.out.println("File not found");
				return false;
			}
					
			in = fs.open(srcPath);
			if(in == null) return false;
			
			dest = dest + "/" + src.substring((src.lastIndexOf("/") + 1));
			out = new BufferedOutputStream(new FileOutputStream(dest));
			
			
			IOUtils.copyBytes(in, out, conf);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			in.close();
			out.close();
			fs.close();
		}
		return true;
		
		
	}
	/**
	 * Print the details of all files (path, blocksize, owner)
	 * @param src file/folder in HDFS
	 * @throws IOException
	 */
	
	public static void PrintFileStatus(String src) throws IOException
	{
		FileSystem fs = null;
		try
		{
			Configuration conf = new Configuration();
			// take this value from core-site.xml/ Use it only when debugging.
			conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020"); 
			fs = FileSystem.get(conf);
			if(fs == null) return;

			Path srcPath = new Path(src);

			PrintFileDetails(fs, srcPath);

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			fs.close();		
		}
	}
	
	private static void PrintFileDetails(FileSystem fs, Path src) throws FileNotFoundException, IOException
	{
		FileStatus[] stats = fs.listStatus(src);
		
		for(FileStatus status : stats)
		{
			if(status.isDirectory())
			{
				System.out.println(status.getPath().toString() + "Directory\n");
				System.out.println("---------------------------\n");
				PrintFileDetails(fs, status.getPath());
			}
			else
			{
				if(status.isFile())
				{
					String strStatus=String.format(" Full Path = %s\n Block Size = %d\n Owner = %s\n", 
							status.getPath().toString(), status.getBlockSize(), status.getOwner());
						  System.out.println(strStatus);
				}
				
			}
		}
	}
	
	
	
    
}
