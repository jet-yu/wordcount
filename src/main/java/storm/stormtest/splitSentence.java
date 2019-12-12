package storm.stormtest;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class splitSentence extends BaseRichBolt {

    OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        System.out.println("sentence:"+sentence);
        if("how do you do".equals(sentence)){
            outputCollector.fail(tuple);
        }else{
            for (String word :sentence.split(" ")){
                outputCollector.emit("split_stream",new Values(word));
            }
            outputCollector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declareStream("split_stream",new Fields("word"));
    }
}
