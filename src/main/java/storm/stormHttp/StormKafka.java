package storm.stormHttp;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;

//从卡夫卡到Storm里
public class StormKafka {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        BrokerHosts brokerHosts = new ZkHosts("master:2181");
        String topic = "topic_kafka_storm_http";
        String zkRoot = "/topic_kafka_storm_http";
        String spoutId = "kafkaSpout";
        SpoutConfig kafkaConf = new SpoutConfig(brokerHosts,topic,zkRoot,spoutId);
        kafkaConf.forceFromStart =true;
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);
        TopologyBuilder topologyBuilder =new TopologyBuilder();
        topologyBuilder.setSpout("spout",kafkaSpout,2);
        topologyBuilder.setBolt("feature_extract",new FeatureExtractBolt()).shuffleGrouping("spout");

        Config config = new Config();
        if(args!=null&&args.length>0){
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
        }else{
            config.setMaxTaskParallelism(3);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("kafka",config,topologyBuilder.createTopology());
        }
    }
}
