package com.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.storm.util.HBaseDAO;

/**
 * Created by konglu on 2016/8/3.
 */
public class UVSumBolt extends BaseBasicBolt{
    private long last=System.currentTimeMillis()/(1000*60);
    private long uv=0;
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        uv+= (Long)input.getValueByField("uv");
        if(!(System.currentTimeMillis()/(1000*60)==last)){
            last=System.currentTimeMillis()/(1000*60);
            HBaseDAO.put("storm", Long.toString(last), "info", "uv", Long.toString(uv));
            uv=0;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
