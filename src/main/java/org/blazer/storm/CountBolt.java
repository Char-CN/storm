package org.blazer.storm;

import java.util.Map;
import java.util.HashMap;

import backtype.storm.tuple.Tuple;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.task.TopologyContext;

public class CountBolt implements IRichBolt {
	private static final long serialVersionUID = 2457348557101185027L;

	Map<String, Integer> counters;
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
	}

	public void execute(Tuple input) {
		System.out.println("==========================================CountBolt execute begin");
		String str = input.getString(0);

		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}

		collector.ack(input);
		System.out.println("==========================================CountBolt execute begin");
	}

	public void cleanup() {
		for (Map.Entry<String, Integer> entry : counters.entrySet()) {
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
