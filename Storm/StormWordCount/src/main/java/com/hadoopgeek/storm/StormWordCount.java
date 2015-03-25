package com.hadoopgeek.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;



/**
 * Hello world!
 *
 */
public class StormWordCount 
{
    public static void main( String[] args ) throws InterruptedException
    {
    	if(args.length < 1)
    	{
    		System.out.println("Invalid Commandline arguments");
    		return;
    	}
    	Config config = new Config();
        config.put("input_file", args[0]);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("file_reader_spout", new FileReaderSpout());
        builder.setBolt("word_splitter_bolt", new WordSplitterBolt()).
        				shuffleGrouping("file_reader_spout");
        builder.setBolt("word_counter_bolt", new WordCounterBolt()).
        				shuffleGrouping("word_splitter_bolt");
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("StormWordCount", config, builder.createTopology());
        
        Thread.sleep(10000);
        
        cluster.shutdown();
    	
    }
}
