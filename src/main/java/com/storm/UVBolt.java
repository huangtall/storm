package com.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RotatingMap;
import com.storm.util.HBaseDAO;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by konglu on 2016/7/29.
 */
public class UVBolt extends BaseRichBolt {
    private static final long serialVersionUID=1l;
    private OutputCollector collector=null;
    private TopologyContext context=null;
    private static Logger LOG= LoggerFactory.getLogger(UVBolt.class);
    private long last=System.currentTimeMillis()/(1000*60);
    private long uv=0;
    private Map<String,Long> map=new HashMap<String, Long>();
    private Map<String,Long> map_tmp=new HashMap<String, Long>();
    private byte[] cf= Bytes.toBytes("info");
    private byte[] col=Bytes.toBytes("time");

    private RotatingMap<String, Long> rmap;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        this.collector=collector;
        this.rmap = new RotatingMap<String, Long>(2);

    }

    @Override
    public void execute(Tuple input) {
        String bid=input.getStringByField("bid");
        long ts=input.getLongByField("ts");

//        if(StringUtils.isNotBlank(bid)){
//            if(map.size()>100000){
//                map=new HashMap<String, Long>();
//            }
//            if(!map.containsKey(bid)){
//                map.put(bid,ts);
//                try {
//                    Result rs= HBaseDAO.get("storm_bid",bid);
//                    if(rs==null){
//                        uv++;
//                    }else {
//                        Cell cell=rs.getColumnLatestCell(cf,col);
//                        if(cell==null){
//                            uv++;
//                        }else {
//                            byte[] time_bytes=cell.getValueArray();
//                            long time=Bytes.toLong(time_bytes)/(1000*60);
//                            if(time==last){
//                                //do nothing
//                            }else {
//                                uv++;
//                            }
//                        }
//                    }
//                } catch (Exception e) {
//                    //e.printStackTrace();
//                    LOG.error(e.getMessage(),e);
//                    this.collector.fail(input);
//                }
//            }else{
//                long tmp=map.get(bid)/(1000*60);
//                if(tmp==last){
//                    //do nothing
//                }else {
//                    uv++;
//                }
//                map.put(bid,ts);
//            }
//            HBaseDAO.put("storm_bid",bid,"info","time",Long.toString(ts));
//        }

        if(StringUtils.isNotBlank(bid)){
            if (map.containsKey(bid)) {
                long tmp=map.get(bid)/(1000*60);
                if(tmp==last){
                    //do nothing
                }else {
                    uv++;
                }
                map.put(bid,ts);
            } else if (map_tmp.containsKey(bid)){
                long tmp=map_tmp.get(bid)/(1000*60);
                if(tmp==last){
                    //do nothing
                }else {
                    uv++;
                }
                map_tmp.put(bid,ts);

            }else {
                map.put(bid,ts);
                try {
                    Result rs= HBaseDAO.get("storm_bid",bid);
                    if(rs==null){
                        uv++;
                    }else {
                        Cell cell=rs.getColumnLatestCell(cf,col);
                        if(cell==null){
                            uv++;
                        }else {
                            byte[] time_bytes=cell.getValueArray();
                            long time=Bytes.toLong(time_bytes)/(1000*60);
                            if(time==last){
                                //do nothing
                            }else {
                                uv++;
                            }
                        }
                    }
                } catch (Exception e) {
                    //e.printStackTrace();
                    LOG.error(e.getMessage(),e);
                    this.collector.fail(input);
                }
            }
            HBaseDAO.put("storm_bid", bid, "info", "time", Long.toString(ts));
            if (map.size() > 100000) {
                Map tmp = map_tmp;
                map_tmp = map;
                tmp.clear();
                map = tmp;
            }

            this.collector.emit(new Values(uv));

        }

        this.collector.ack(input);
        if(!(System.currentTimeMillis()/(1000*60)==last)){
            last=System.currentTimeMillis()/(1000*60);
            HBaseDAO.put("storm",Long.toString(last),"info","uv",Long.toString(uv));
            uv=0;
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
