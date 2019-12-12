package storm.stormtest;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

import static java.lang.Thread.sleep;

public class wordcountspout extends BaseRichSpout implements IRichSpout {

    SpoutOutputCollector spoutOutputCollector;
    Random rand;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;

    }

    @Override
    public void nextTuple() {
        // TODO: 2019/12/6  dosomething
        String[] words = new String[]{"how do you do","you do what","do you know"};
        rand = new Random();
        String word = words[rand.nextInt(words.length)];
        Object msgid = rand.hashCode();
        System.out.println("msgid"+msgid.toString());

        spoutOutputCollector.emit("spout_stream",new Values(word),msgid);

        try {
            sleep(1000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("Recive ACK!!!, msgid: "+ msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("Recive Fail!!!, msgid: " + msgId);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("spout_stream", new Fields("sentence"));
    }
}
