package org.blazer.storm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class MyKafkaSpout extends BaseRichSpout {

	private static final long serialVersionUID = 8284318790249361525L;
	private static final ConsumerConnector consumer;
	private static final String ZK_HOST = "ms:2181,sl1:2181,sl2:2181";
	private static final String TOPIC = "my-topic";
	private static final String TOPIC_GROUP_ID = "hyy-group";
	ConsumerIterator<String, String> it;
	SpoutOutputCollector collector;

	static {
		Properties props = new Properties();
		props.put("zookeeper.connect", ZK_HOST);
		props.put("group.id", TOPIC_GROUP_ID);
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ConsumerConfig config = new ConsumerConfig(props);
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
	}

	private void consumer() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TOPIC, new Integer(1));
		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
		Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
		KafkaStream<String, String> stream = consumerMap.get(TOPIC).get(0);
		ConsumerIterator<String, String> it = stream.iterator();
		this.it = it;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.consumer();
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
			collector.emit(new Values(msg));
		}
	}

}
