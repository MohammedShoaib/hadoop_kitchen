package com.hadoopgeek.storm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordSplitterBolt extends BaseRichBolt
{
	OutputCollector _collector;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector)
	{
		_collector = collector;		
	}

	public void execute(Tuple input)
	{
		String line = input.getString(0);
		String[] words = line.split(" ");
		
		for(String word : words)
		{
			if(!word.isEmpty())
			_collector.emit(new Values(word.trim()));
		}
		_collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("word"));
		
	}

}
