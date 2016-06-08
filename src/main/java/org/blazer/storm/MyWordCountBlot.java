package org.blazer.storm;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class MyWordCountBlot extends BaseBasicBolt {

	private static final long serialVersionUID = 1461988131175366942L;
	private HashMap<String, Integer> counters = new HashMap<String, Integer>();
	private volatile boolean edit = false;

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					if (edit) {
						for (Entry<String, Integer> entry : counters.entrySet()) {
							System.out.println(entry.getKey() + " : " + entry.getValue());
						}
						System.out.println("WordCounter---------------------------------------");
						edit = false;
					}
					try {
						Thread.sleep(1 * 1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}).start();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String str = input.getString(0);
		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
		edit = true;
		System.out.println("WordCounter+++++++++++++++++++++++++++++++++++++++++++");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
