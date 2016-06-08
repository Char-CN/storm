package org.blazer.storm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class MyKafkaConsumer {

	private BlockingQueue<String> queue = new ArrayBlockingQueue<String>(10);

	private final ConsumerConnector consumer;
	private final String TOPIC = "my-topic";

	public MyKafkaConsumer() {
		Properties props = new Properties();
		props.put("zookeeper.connect", "ms:2181,sl1:2181,sl2:2181");
		props.put("group.id", "hyy-group");
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		// 序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ConsumerConfig config = new ConsumerConfig(props);
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
	}

	public void consume() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TOPIC, new Integer(1));
		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
		Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
		KafkaStream<String, String> stream = consumerMap.get(TOPIC).get(0);
		ConsumerIterator<String, String> it = stream.iterator();
		while (it.hasNext()) {
			String msg = it.next().message();
			try {
				this.queue.put(msg);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println(msg);
		}
	}

	public String poll() {
		return queue.poll();
	}

	public static void main(String[] args) {
		new MyKafkaConsumer().consume();
	}

}