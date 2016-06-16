package org.blazer.storm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.blazer.common.conf.Conf;
import org.blazer.common.conf.ConfUtil;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class KafkaSpout extends BaseRichSpout {

	private static final long serialVersionUID = 8284318790249361525L;
	ConsumerIterator<String, String> it;
	SpoutOutputCollector collector;

	@Override
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		Conf myconf = ConfUtil.getConf("/topology.conf");
		Properties props = new Properties();
		props.put("zookeeper.connect", myconf.get("zookeeper.connect"));
		props.put("group.id", myconf.get("group.id"));
		props.put("zookeeper.session.timeout.ms", myconf.get("zookeeper.session.timeout.ms"));
		props.put("zookeeper.sync.time.ms", myconf.get("zookeeper.sync.time.ms"));
		props.put("auto.commit.interval.ms", myconf.get("auto.commit.interval.ms"));
		props.put("auto.offset.reset", myconf.get("auto.offset.reset"));
		props.put("serializer.class", StringDecoder.class.getName());
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(myconf.get("topic"), 1);
		StringDecoder key = new StringDecoder(new VerifiableProperties());
		StringDecoder value = new StringDecoder(new VerifiableProperties());
		Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, key, value);
		KafkaStream<String, String> stream = consumerMap.get(myconf.get("topic")).get(0);
		ConsumerIterator<String, String> it = stream.iterator();
		this.it = it;
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("kafka_message"));
	}

	@Override
	public void nextTuple() {
		if (it.hasNext()) {
			String msg = it.next().message();
			this.collector.emit(new Values(msg));
		}
		Utils.sleep(10);
	}

}
