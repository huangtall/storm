package com.storm;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by konglu on 2016/7/29.
 * 多线程计算pv
 */
public class PVBolt extends BaseRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(PVBolt.class);
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private TopologyContext context;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.context = topologyContext;
        this.collector = outputCollector;
    }
    @Override
    public void execute(Tuple tuple) {
        try {
            byte[] buffer= (byte[]) tuple.getValueByField("bytes");
            String strs=new String(buffer);
            JSONObject json= JSON.parseObject(strs);
            String bid= (String) json.get("browser_uniq_id");
            this.collector .emit(tuple, new Values(bid,System.currentTimeMillis()));
            this.collector .ack(tuple);
        } catch(Exception e ){
            //e.printStackTrace();
            LOG.error(e.getMessage(), e);
            this.collector .fail(tuple );
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare( new Fields("bid","ts"));
    }
}
