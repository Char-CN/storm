package org.blazer.storm;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MyKafkaSpout2 extends BaseRichSpout {

	private static final long serialVersionUID = -5721126401702648403L;
	private SpoutOutputCollector collector;
	private MyKafkaConsumer consumer;

    public void fail(Object msgId) {
        System.out.println("KafkaReaderSpout fail(),failed msg :" + msgId.toString());
    }

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.consumer = new MyKafkaConsumer();
		this.consumer.consume();
	}

	@Override
	public void nextTuple() {
		this.collector.emit(new Values(this.consumer.poll()));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("kafka_message"));
	}

}
