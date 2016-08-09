package com.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by konglu on 2016/7/29.
 */
public class UVTopology {
    private static String SPOUT_ID= KafkaSpout.class.getSimpleName();
    private static String PVBOLT_ID=PVBolt.class.getSimpleName();
    private static String UVBOLT_ID=UVBolt.class.getSimpleName();
    private static String UVTOPOLOGY_ID=UVTopology.class.getSimpleName();
    private static Logger LOG= LoggerFactory.getLogger(UVTopology.class);
    private static String UVSUMBOLT_ID=UVSumBolt.class.getSimpleName();

    public static void main(String ... args){
        TopologyBuilder builder=new TopologyBuilder();
        String brokerZkStr = "172.19.176.49:2181,172.19.176.50:2181,172.19.176.51:2181,172.19.176.52:2181,172.19.176.53:2181/kafka";
        String zkRoot = "/kafka";
        ZkHosts zkHosts = new ZkHosts(brokerZkStr);
        String topic = "flow_normalized_json";
        String id = UUID.randomUUID().toString();
        SpoutConfig spoutconf  = new SpoutConfig(zkHosts, topic, zkRoot, id);

        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutconf), 1);
        builder.setBolt(PVBOLT_ID,new PVBolt(),16).shuffleGrouping(SPOUT_ID);
        builder.setBolt(UVBOLT_ID,new UVBolt(),1).shuffleGrouping(PVBOLT_ID);
        //builder.setBolt(UVSUMBOLT_ID,new UVSumBolt(),1).shuffleGrouping(UVBOLT_ID);

        Config conf = new Config();
        conf.setMaxSpoutPending(1000);
        conf.setStatsSampleRate(1.0);
        conf.setNumAckers(3);

        if(args!=null && args.length>0){
            try {
                StormSubmitter.submitTopology(UVTOPOLOGY_ID, conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                LOG.error(e.getMessage(),e);
            } catch (InvalidTopologyException e) {
                LOG.error(e.getMessage(),e);
            }
        }else {
            LocalCluster cluster=new LocalCluster();
            cluster.submitTopology(UVTOPOLOGY_ID, conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology(UVTOPOLOGY_ID);
            cluster.shutdown();
        }
    }

}
