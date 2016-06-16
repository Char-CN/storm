package org.blazer.storm;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitWordBlot extends BaseBasicBolt {

	private static final long serialVersionUID = -2283010744344645087L;
	private Logger logger = LoggerFactory.getLogger(KafkaSpout.class);

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
		String[] words = line.split(" ");
		for (String word : words) {
			word = word.trim();
			if (StringUtils.isNotBlank(word)) {
				word = word.toLowerCase();
				collector.emit(new Values(word));
			}
		}
		Integer.parseInt("asd");
	}
	
	@Override
	public void cleanup() {
		super.cleanup();
		logger.info("============================cleanup=============================");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
