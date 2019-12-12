package storm.stormKafkaPackage;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;


public class stormKafka {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        String topic = "badou_storm_kafka_test";
        String zkRoot = "/badou_storm_kafka_test";
        String spoutId = "kafkaSpout";
        BrokerHosts brokerHosts = new ZkHosts("master:2181");
        SpoutConfig kafkaConf = new SpoutConfig(brokerHosts,topic,zkRoot,spoutId);
        kafkaConf.forceFromStart = true;
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",kafkaSpout,2);
        builder.setBolt("printer",new PrinterBolt())
                .shuffleGrouping("spout");

        Config config = new Config();
        config.setDebug(false);
        if(args!=null&&args.length >0){
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }else{
            config.setMaxTaskParallelism(3);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("kafka",config,builder.createTopology());
        }
    }

}
