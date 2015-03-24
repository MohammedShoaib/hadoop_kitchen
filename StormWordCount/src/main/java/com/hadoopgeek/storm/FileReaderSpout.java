package com.hadoopgeek.storm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileReaderSpout extends BaseRichSpout
{
	private FileReader _fileReader;
	private SpoutOutputCollector _collector;
	private Boolean _completed;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector)
	{
		_completed = false;
		try
		{
			_fileReader = new FileReader(conf.get("input_file").toString());
			_collector = collector;		

		} catch (FileNotFoundException e)
		{

			e.printStackTrace();
		}
	}

	public void nextTuple()
	{
		if (_completed)
		{
			try
			{
				Thread.sleep(1000);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}

		BufferedReader reader = new BufferedReader(_fileReader);
		String line;
		try
		{
			while ((line = reader.readLine()) != null)
			{
				_collector.emit(new Values(line), line);
			}
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally
		{
			_completed = true;
		}
	}

	@Override
	public void close()
	{
		super.close();
		try
		{
			_fileReader.close();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("line"));

	}

}
