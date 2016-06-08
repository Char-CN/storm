package org.blazer.storm;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class MySpout extends BaseRichSpout {

	private static final long serialVersionUID = -1423621091088755757L;
	SpoutOutputCollector _collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		final String[] words = new String[] { "nathan", "mike", "jackson", "golda", "bertels" };
		final Random rand = new Random();
		final String word = words[rand.nextInt(words.length)];
		_collector.emit(new Values(word));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
