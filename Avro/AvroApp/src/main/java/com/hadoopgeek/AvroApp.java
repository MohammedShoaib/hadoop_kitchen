package com.hadoopgeek;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;





import java.net.URL;
import java.util.Scanner;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
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
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Hello world!
 *
 */
public class AvroApp 
{
    public static void main( String[] args ) throws IOException
    {
       SpecificAPI();
    	//GenericAPI();
    }
    
    public static void GenericAPI() throws IOException
    {
    	
    	//ClassLoader classLoader = AvroApp.class.getClassLoader();
    	//File file = new File(classLoader.getResource("book.avsc").getFile());
    	    	
    	AvroApp.class.getClassLoader().getResourceAsStream("book.avsc");
    	//FileInputStream fis = new FileInputStream(file);
    	Schema.Parser parser = new Schema.Parser();
    	Schema schema = parser.parse(AvroApp.class.getClassLoader().getResourceAsStream("book.avsc"));
    	
    	GenericRecord datum = new GenericData.Record(schema); 
    	
    	datum.put("name", "HadoopBook");
    	datum.put("id", 109);
    	datum.put("category", "Programming");
    	
    	datum.put("name", "Had2");
    	datum.put("id", 107);
    	datum.put("category", "Programming");
    	
    	datum.put("name", "HadoopBook2");
    	datum.put("id", 101);
    	datum.put("category", "Programming");
    	
    	datum.put("name", "HadoopBook4");
    	datum.put("id", 103);
    	datum.put("category", "Programming");
    	
    	
    	
    	ByteArrayOutputStream out = new ByteArrayOutputStream(); 
    	DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema); 
    	Encoder encoder = EncoderFactory.get().binaryEncoder(out, null); 
    	writer.write(datum, encoder);
    	encoder.flush();
    	out.close();
    	
    	//deserialize
    	
    	DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema); 
    	Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
     	GenericRecord result = reader.read(null, decoder); 
    	System.out.println(result.get("name").toString());
    	System.out.println(result.get("id").toString());
    	System.out.println(result.get("category").toString()); 
    	
    	// Read using a diffrent schema
    	Schema.Parser parser1 = new Schema.Parser();
    	Schema schema1 = parser1.parse(AvroApp.class.getClassLoader().getResourceAsStream("book1.avsc"));
    	DatumReader<GenericRecord> reader1 = new GenericDatumReader<GenericRecord>(schema,schema1); // if data file then can pass null for writer schema
    	Decoder decoder1 = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    	GenericRecord result1 = reader1.read(null, decoder1); 
    	System.out.println(result1.get("name").toString());
    	System.out.println(result1.get("id").toString());
    	System.out.println(result1.get("category").toString()); 
    	if(result1.get("description") != null)
    		System.out.println(result1.get("description").toString()); 
  	   	
    }
    
	public static void SpecificAPI() throws IOException 
	{
		Book book1 = Book.newBuilder().setId(123).setName("Programming is fun")
				.setCategory("Fiction").build();
		Book book2 = new Book("Some book", 456, "Horror");
		Book book3 = new Book();
		book3.setName("And another book");
		book3.setId(789);
		//File store = File.createTempFile("book", ".avro");
 
		File store = new File("book.avro");
		
		// serializing
		
		System.out.println("serializing books to  file: " + store.getPath());
		
		DatumWriter<Book> bookDatumWriter = new SpecificDatumWriter<Book>(Book.class);
		DataFileWriter<Book> bookFileWriter = new DataFileWriter<Book>(bookDatumWriter);
		bookFileWriter.create(book1.getSchema(), store);
		bookFileWriter.append(book1);
		bookFileWriter.append(book2);
		bookFileWriter.append(book3);
		bookFileWriter.close(); 
		
		System.out.println("serializing books to  file: " + store.getPath() + "completed");
		
		//deserializing
		
		System.out.println("deserializing books from  file: " + store.getPath());
		DatumReader<Book> bookDatumReader = new SpecificDatumReader<Book>(
				Book.class);
		DataFileReader<Book> bookFileReader = new DataFileReader<Book>(store, bookDatumReader);
		while (bookFileReader.hasNext()) {
			Book b1 = bookFileReader.next();
			System.out.println("deserialized from file: " + b1);
	}
  }
}
