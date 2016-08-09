package com.storm.util;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by konglu on 2016/7/25.
 *
 */
public class HBaseDAO {
    private static Logger LOG= LoggerFactory.getLogger(HBaseDAO.class);
    private static HBaseUtils hBaseUtils=new HBaseUtils("172.22.96.56",2181,"/hbase");
    public static void put(String tablename, String row, String columnFamily,
                           String column, String data)  {

        HTable table = hBaseUtils.getTable(tablename);
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(data));
        try {
            table.put(put);
            table.close();
        } catch (IOException e) {
            LOG.error(e.getMessage(),e);
        }

    }

    public static Result get(String tablename, String row) throws Exception {
        HTable table = hBaseUtils.getTable(tablename);
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        table.close();
        return result;
    }

    public static ResultScanner scan(String tablename) throws Exception {

        HTable table =hBaseUtils.getTable(tablename);
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);
        return rs;
    }
    public static ResultScanner containKeys(String tablename,String rowkey) throws IOException, IllegalAccessException, InstantiationException {
        HTable table = hBaseUtils.getTable(tablename);
        Scan scan=new Scan();
        Filter filter=new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(rowkey));
        scan.setFilter(filter);
        return table.getScanner(scan);
    }
}
