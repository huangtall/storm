package com.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.storm.util.HBaseDAO;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by konglu on 2016/7/29.
 */
public class PVSumBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private Map<Integer,Long> map = new HashMap<Integer,Long>();
    private static Logger LOG= LoggerFactory.getLogger(PVBolt.class);
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.last = System.currentTimeMillis()/(1000*60);
    }

    private long pv;
    private long last;

    @Override
    public void execute(Tuple tuple) {
        try {
            String bid=tuple.getStringByField("bid");
            if(StringUtils.isNotBlank(bid)){
                pv++;
            }
            if(System.currentTimeMillis()/(1000*60)!= last) {
                last = System.currentTimeMillis()/(1000*60);
                HBaseDAO.put("storm",Long.toString(last),"info","pv",Long.toString(pv));
                pv=0;
            }else {
                //do nothing
            }
            this.collector.ack(tuple);
        }catch(Exception e){
            //e.printStackTrace();
            LOG.error(e.getMessage(),e);
            this.collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
