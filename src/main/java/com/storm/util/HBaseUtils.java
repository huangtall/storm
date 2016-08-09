package com.storm.util;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by konglu on 2016/7/27.
 */
public class HBaseUtils {
    private Logger LOG = LoggerFactory.getLogger(HBaseUtils.class);

    private Configuration configuration;
    private Connection connection;
    public HBaseUtils(){
        this.configuration = HBaseConfiguration.create();
    }
    public HBaseUtils(String zkServers, int zkPort, String zkRoot) {
        this.configuration = HBaseConfiguration.create();
        this.configuration.set("hbase.zookeeper.quorum", zkServers);
        this.configuration.set("hbase.zookeeper.property.clientPort", zkPort + "");
        this.configuration.set("zookeeper.znode.parent", zkRoot);
    }

    public synchronized Connection getHConnection()
            throws IOException {
        if (connection == null) {
            connection = ConnectionFactory.createConnection(configuration);
//            connection = HConnectionManager.createConnection(configuration);
        }
        return connection;
    }

    public HTable getTable(String tableName) {
        HTable table = null;
        try {
            if (null == connection) {
                connection = getHConnection();
            }
            table = (HTable) connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }
        if (null == table) {
            throw new RuntimeException(" can not connect HBase: exception accurs when getting table from hconnection " + tableName);
        }
        return table;
    }
}
