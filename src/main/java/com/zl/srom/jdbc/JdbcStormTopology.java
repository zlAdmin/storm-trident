package com.zl.srom.jdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.shade.org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class JdbcStormTopology {
    public static class DataSourceSpout extends BaseRichSpout{
        public static final String[] words =new String[] {"apple","orange","pineapple"};
        private SpoutOutputCollector collection;

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collection = collector;
        }

        public void nextTuple() {
            Random random = new Random();
            String word = words[random.nextInt(words.length)];
            System.out.println("输入为："+word);
            this.collection.emit(new Values(word));
            Utils.sleep(1000);
            
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
        
    }
    public static class SpiltBolt extends BaseRichBolt{
        private OutputCollector collector;

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            
        }

        public void execute(Tuple input) {
            String value = input.getStringByField("line");
            this.collector.emit(new Values(value));
            
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

            declarer.declare(new Fields("word"));
        }
        
    }
    public static class CountBolt extends BaseRichBolt{
        private OutputCollector collector;
        Map<String ,Integer> map = new HashMap<String,Integer>();

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            Integer count = map.get(word);
            if(count==null) {
                count = 0;
            }
            count++;
            map.put(word, count);
            this.collector.emit(new Values(word,map.get(word)));
            
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","word_count"));
            
        }
        
    }
    
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("dataSourceSpout", new DataSourceSpout());
        builder.setBolt("spiltBolt", new SpiltBolt()).shuffleGrouping("dataSourceSpout");
        builder.setBolt("countBolt", new CountBolt()).shuffleGrouping("spiltBolt");
        
        
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/test");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","123456");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
        
        String tableName = "word_count";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                                            .withTableName(tableName)
                                            .withQueryTimeoutSecs(30);
        builder.setBolt("jdbcInsertBolt", userPersistanceBolt).shuffleGrouping("countBolt");                              
       
        
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("RedisStormTopology", new Config(), builder.createTopology());
        
    }

}
