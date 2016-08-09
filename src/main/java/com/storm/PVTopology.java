package com.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by konglu on 2016/7/29.
 */
public class PVTopology {
    public final static String SPOUT_ID = KafkaSpout.class.getSimpleName();
    public final static String PVBOLT_ID = PVBolt.class.getSimpleName();
    public final static String PVTOPOLOGY_ID = PVTopology.class.getSimpleName();
    public final static String PVSUMBOLT_ID = PVSumBolt.class.getSimpleName();
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        String brokerZkStr = "172.19.176.49:2181,172.19.176.50:2181,172.19.176.51:2181,172.19.176.52:2181,172.19.176.53:2181/kafka";
        String zkRoot = "/kafka";
        ZkHosts zkHosts = new ZkHosts(brokerZkStr);
        String topic = "flow_normalized_json";
        String id = UUID.randomUUID().toString();
        SpoutConfig spoutconf  = new SpoutConfig(zkHosts, topic, zkRoot, id);

        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutconf), 1);
        builder.setBolt( PVBOLT_ID, new PVBolt(), 4).shuffleGrouping(SPOUT_ID);
        builder.setBolt( PVSUMBOLT_ID, new PVSumBolt(), 1).shuffleGrouping(PVBOLT_ID);

        Map<String,Object> conf = new HashMap<String,Object>();
        conf.put(Config. TOPOLOGY_RECEIVER_BUFFER_SIZE , 8);
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);

        if(args!=null && args.length>0){
            StormSubmitter.submitTopology(PVTOPOLOGY_ID, conf, builder.createTopology());
        }else {
            LocalCluster cluster=new LocalCluster();
            cluster.submitTopology(PVTOPOLOGY_ID, conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology(PVTOPOLOGY_ID);
            cluster.shutdown();
        }


    }
}
