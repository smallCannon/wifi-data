package cn.itcast.cloud;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopology  {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        KafkaSpoutConfig.Builder<String, String> builder = KafkaSpoutConfig.builder("node-1:9092,node-2:9092,node-3:9092", "wifidata");
        builder.setGroupId("wifida哦taGroup");
        builder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST);
        KafkaSpoutConfig<String, String> config = builder.build();
        KafkaSpout spout = new KafkaSpout(config);
        topologyBuilder.setSpout("kafkaSpout",spout);
        topologyBuilder.setBolt("WifiTypeBolt", new wifiBolt()).localOrShuffleGrouping("kafkaSpout");
        topologyBuilder.setBolt("wifiWarningBolt", new wifiWarnongBolt()).localOrShuffleGrouping("WifiTypeBolt");
        topologyBuilder.setBolt("writeFileBolt", new writeFileBolt()).localOrShuffleGrouping("wifiWarningBolt");
        Config submitConfig = new Config();
        if(args !=null && args.length > 0){
            submitConfig.setDebug(false);
            StormSubmitter submitter = new StormSubmitter();
            submitter.submitTopology(args[0],submitConfig,topologyBuilder.createTopology());
        }else{
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wifiDataMyPlatForm",submitConfig,topologyBuilder.createTopology());
        }
    }

}
