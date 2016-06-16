package org.blazer.storm;

import org.blazer.common.conf.Conf;
import org.blazer.common.conf.ConfUtil;
import org.blazer.common.util.IntegerUtil;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class TopologyMain {

	public static void main(String[] args) throws Exception {
		Conf myconf = ConfUtil.getConf("/topology.conf");
		Integer KafkaSpoutNumber = IntegerUtil.parseInt0(myconf.get("KafkaSpoutNumber"));
		Integer SplitWordBlotNumber = IntegerUtil.parseInt0(myconf.get("SplitWordBlotNumber"));
		Integer WordCountBlotNumber = IntegerUtil.parseInt0(myconf.get("WordCountBlotNumber"));

		// Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", new KafkaSpout(), KafkaSpoutNumber);
		builder.setBolt("SplitWordBlot", new SplitWordBlot(), SplitWordBlotNumber).shuffleGrouping("KafkaSpout");
		builder.setBolt("WordCountBlot", new WordCountBlot(), WordCountBlotNumber).shuffleGrouping("SplitWordBlot");

		// Configuration
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(7);
		StormSubmitter.submitTopology("TopologyMain", conf, builder.createTopology());
	}

}
