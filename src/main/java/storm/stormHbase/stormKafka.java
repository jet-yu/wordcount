package storm.stormHbase;

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
        BrokerHosts brokerHosts = new ZkHosts("master:2181");
        String topic = "badou_kafka_storm_hbase";
        String zkRoot ="/badou_kafka_storm_hbase";
        String spoutId = "kafakSpout";
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,topic,zkRoot,spoutId);
        spoutConfig.forceFromStart =true;//
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",kafkaSpout,2);
        builder.setBolt("printer",new PrinterBolt()).shuffleGrouping("spout");

        Config config = new Config();
        config.setDebug(true);
        if(args!=null&&args.length>0){
            //config.setMaxTaskParallelism(3);
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());

        }else{
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("kafka",config,builder.createTopology());
        }
    }
}
