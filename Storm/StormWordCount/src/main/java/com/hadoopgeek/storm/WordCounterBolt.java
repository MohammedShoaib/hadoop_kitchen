package com.hadoopgeek.storm;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WordCounterBolt extends BaseRichBolt
{
	Map<String,Integer>_map;
	
	@Override
	public void cleanup()
	{
		super.cleanup();
		for(Entry<String, Integer> item : _map.entrySet())
		{
			System.out.println(item.getKey() + "-" + item.getValue());
		}
		
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector)
	{
		_map = new HashMap<String, Integer>();
	}

	public void execute(Tuple input)
	{
		 String word = input.getString(0);
		 if(_map.containsKey(word))
		 {
			 Integer count = _map.get(word);
			 count++;
			 _map.put(word, count);
		 }
		 else
		 {
			 _map.put(word, 1);
		 }
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		// TODO Auto-generated method stub
		
	}

}
