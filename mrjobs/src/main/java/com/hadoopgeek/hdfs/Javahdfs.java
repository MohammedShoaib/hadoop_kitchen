package com.hadoopgeek.hdfs;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class Javahdfs 
{
	public static void main(String[] args) throws IOException 
	{
		PrintClassPath();
		ReadFromHDFS(args[0], args[1]);
		//HDFStoHDFSCopy(args[0], args[1]);
		//WritetoHDFS(args[0], args[1]);
		//Delete(args[0]);
		//ListStatus(args);
	}

	// This code copy a file from HDFS to local . You can use System.out as outstream if you need to display in console
	public static void ReadFromHDFS(String src, String dest) throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(src),conf);
		System.out.println(URI.create(src).toString());
		InputStream in = null;
		OutputStream out = null;
		try
		{
			in = hdfs.open(new Path(src));
			//String fileName = dest.substring(dest.lastIndexOf('/') + 1, dest.length());
			out = new BufferedOutputStream(new FileOutputStream(dest));
			byte[] b = new byte[1024];
			int numBytes = 0;
			while((numBytes = in.read(b)) > 0)
			{
				out.write(b, 0, numBytes);
			}
		}
		finally
		{
			in.close();
			out.close();
			hdfs.close();
		}
		System.out.println("Copy completed");
	}

	// Copy a file in HDFS to another HDFS location or another file.
	public static void HDFStoHDFSCopy(String src, String dest) throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(src),conf);
		if(hdfs == null) return;
		InputStream in = null;
		OutputStream out = null;
		try
		{
			out = hdfs.create(new Path(dest));
			in = hdfs.open(new Path(src));
			byte[] b = new byte[1024];
			int numBytes = 0;
			while((numBytes = in.read(b)) > 0)
			{
				out.write(b, 0, numBytes);
			}
		}
		finally
		{
			in.close();
			out.close();
			hdfs.close();
		}

	}

	// Write a new file into HDFS (Copy To HDFS)
	public static void WritetoHDFS(String src, String dest) throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(dest),conf);
		if(hdfs == null) return;
		InputStream in = null;
		OutputStream out = null;
		try 
		{
			in = new BufferedInputStream(new FileInputStream(src));
			out = hdfs.create(new Path(dest), new Progressable() 
			{

				public void progress() 
				{
					System.out.println(".");

				}

			}
					);
			byte[] b = new byte[1024];
			int numBytes = 0;
			while((numBytes = in.read(b)) > 0)
			{
				out.write(b, 0, numBytes);
			}
		} 
		finally 
		{
			in.close();
			out.close();
			hdfs.close();
		}

	}

	// Delete a File From HDFS
	public static void Delete(String dest) throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(dest),conf);
		Path outputPath = new Path(dest);
		if(hdfs.exists(outputPath))
			hdfs.delete(outputPath, true);
		System.out.println("Deletion Completed !");
	}

	// List file Status

	public static void ListStatus(String[] locations) throws IOException

	{
		String loc1 = locations[0];
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(loc1),conf);

		Path[] paths = new Path[locations.length];
		for (int i = 0; i < paths.length; i++) 
		{
			paths[i] = new Path(locations[i]);
		}
		FileStatus[] status = hdfs.listStatus(paths);
		for (int i = 0; i < status.length; i++) 
		{
			FileStatus st = status[i];
			String stat = "Path = " + st.getPath().toString() + " Owner = " + st.getOwner() +
					" Replication = " + st.getReplication() + " Block Size = " + st.getBlockSize();

			System.out.println(stat);
		}

	}
	
	public static void PrintClassPath()
	{
		 ClassLoader cl = ClassLoader.getSystemClassLoader();
		 
	        URL[] urls = ((URLClassLoader)cl).getURLs();
	 
	        for(URL url: urls){
	        	System.out.println(url.getFile());
	        }
	}


}
