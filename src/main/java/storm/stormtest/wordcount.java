package storm.stormtest;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class wordcount extends BaseRichBolt {

    OutputCollector outputCollector;
    public Map<String , Integer> countMap = new HashMap<String ,Integer>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        System.out.println("word:"+word);
        Integer count = countMap.get(word);
        if(count == null){
            count = 0;
        }
        count++;
        countMap.put(word,count);
        Iterator<String> iterable = this.countMap.keySet().iterator();
        System.out.println("wordcount最后一步循环打印start");
        while (iterable.hasNext()){
            String next = iterable.next();
            System.out.println(next+":"+countMap.get(next));
        }
        System.out.println("wordcount最后一步循环打印end");
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
