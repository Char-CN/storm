package org.blazer.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class MyTopologyMain {

	public static void main(String[] args) throws Exception {

		// Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("MyKafkaSpout", new MyKafkaSpout(), 3);
		builder.setBolt("MySplitWordBlot", new MySplitWordBlot(), 1).shuffleGrouping("MyKafkaSpout");
		builder.setBolt("MyWordCountBlot", new MyWordCountBlot(), 1).shuffleGrouping("MySplitWordBlot");

		// Configuration
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(4);
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("MyStormProcess", conf, builder.createTopology());
		StormSubmitter.submitTopology("MyStormProcess", conf, builder.createTopology());
	}

}
