package org.blazer.storm;

import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class KafkaStormSample {
	public static void main(String[] args) throws Exception {
		Config config = new Config();
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		String zkConnString = "ms:2181,sl1:2181,sl2:2181";
		String topic = "my-topic";
		BrokerHosts hosts = new ZkHosts(zkConnString);

//		SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
		SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, "/consumers", "hyy-group");
//		kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
//		kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
		kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		TopologyBuilder builder = new TopologyBuilder();
		System.out.println("=======================================================1=================");
		builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));
		System.out.println("=======================================================2=================");
		builder.setBolt("word-spitter", new SplitBolt()).shuffleGrouping("kafka-spout");
		System.out.println("=======================================================3=================");
		builder.setBolt("word-counter", new CountBolt()).shuffleGrouping("word-spitter");
		System.out.println("=======================================================4=================");

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("KafkaStormSample", config, builder.createTopology());
		System.out.println("=======================================================5=================");
//		cluster.shutdown();
	}
}
