package org.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class Main {

	public static void main(String[] args) throws InterruptedException {
		

	     
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("picSpout", new PicSpout(), 1);
	        builder.setBolt("picBolt", new PicBolt(),1).shuffleGrouping("picSpout");
	    
	        StormTopology stormTopology = builder.createTopology();
		       		Config config = new Config();
		            config.setNumWorkers(1);
		            config.setMaxTaskParallelism(2);
		            LocalCluster cluster = new LocalCluster();
		            config.put(Config.NIMBUS_THRIFT_PORT,  6627);
		            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		            cluster.submitTopology("tvc-analyze", config, stormTopology);
		            Thread.sleep(5000000);
		            cluster.shutdown();

	}

}
