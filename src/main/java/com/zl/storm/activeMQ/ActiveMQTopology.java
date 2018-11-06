package com.zl.storm.activeMQ;

import java.util.Map;

import javax.jms.Session;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jms.JmsProvider;
import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.jms.spout.JmsSpout;
import org.apache.storm.shade.org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class ActiveMQTopology {
    public static final String JMS_QUEUE_SPOUT = "JMS_QUEUE_SPOUT";
    public static final String INTERMEDIATE_BOLT = "INTERMEDIATE_BOLT";
    public static final String DEALL_OG_BOLT = "DEALL_OG_BOLT";
    public static final String JMS_TOPIC_BOLT = "JMS_TOPIC_BOLT";
    public static final String JMS_TOPIC_SPOUT = "JMS_TOPIC_SPOUT";
    public static final String ANOTHER_BOLT = "ANOTHER_BOLT";
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void main(String[] args) throws Exception {
        // JMS Queue Provider
        JmsProvider jmsQueueProvider = new SpringJmsProvider(
                "jms-activemq.xml", "jmsConnectionFactory",
                "notificationQueue");
        // JMS Producer
        JmsTupleProducer producer = new JsonTupleProducer();

        // JMS Queue Spout
        JmsSpout queueSpout = new JmsSpout();
        queueSpout.setJmsProvider(jmsQueueProvider);
        queueSpout.setJmsTupleProducer(producer);
        queueSpout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        queueSpout.setDistributed(true); // allow multiple instances

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(JMS_QUEUE_SPOUT, queueSpout, 5);
        builder.setBolt(INTERMEDIATE_BOLT,
            new GenericBolt("INTERMEDIATE_BOLT", true, true, new Fields("json")), 3).shuffleGrouping(
            JMS_QUEUE_SPOUT);
        builder.setBolt(DEALL_OG_BOLT, new DealLogBolt()).shuffleGrouping(
            INTERMEDIATE_BOLT);
        
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/storm");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","123456");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
        String tableName = "position";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                                            .withTableName(tableName)
                                            .withQueryTimeoutSecs(30);
        
        builder.setBolt("jdbcInsertBolt", userPersistanceBolt).shuffleGrouping(DEALL_OG_BOLT);
        
        Config conf = new Config();

        if (args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf,
                    builder.createTopology());
        } else {

            conf.setDebug(true);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm-jms-example", conf, builder.createTopology());
            Utils.sleep(600000);
            cluster.killTopology("storm-jms-example");
            cluster.shutdown();
        }
    }

}
