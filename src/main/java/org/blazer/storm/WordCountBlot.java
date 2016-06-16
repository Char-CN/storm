package org.blazer.storm;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WordCountBlot extends BaseBasicBolt {

	private static final long serialVersionUID = 1461988131175366942L;
	private Logger logger = LoggerFactory.getLogger(WordCountBlot.class);
	private HashMap<String, Integer> counters = new HashMap<String, Integer>();

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String str = input.getString(0);
		Integer count = 1;
		if (counters.containsKey(str)) {
			count += counters.get(str);
		}
		counters.put(str, count);
		logger.info("================== New Count [" + str + "] : " + "[" + count + "]");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// pass
	}

}
