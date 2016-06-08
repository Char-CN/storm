package org.blazer.storm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitBolt implements IRichBolt {

	private static final long serialVersionUID = -8024312375182910497L;

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		System.out.println("==========================================SplitBolt execute begin");
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");
		for (String word : words) {
			word = word.trim();
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				collector.emit(new Values(word));
			}
		}
		collector.ack(input);
		System.out.println("==========================================SplitBolt execute end");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public void cleanup() {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
