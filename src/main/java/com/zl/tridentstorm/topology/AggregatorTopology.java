package com.zl.tridentstorm.topology;

import java.io.IOException;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

import com.zl.trident.function.AggreatorPrintFunction;
import com.zl.trident.spout.FakeTweetsBatchSpout;
import com.zl.tridentstorm.aggregator.LocationAggregator;

public class AggregatorTopology {
    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        try {
            FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(10);
            topology.newStream("spout", spout)
//            .aggregate(new Fields("location"), new LocationAggregator(), new Fields("location_counts"))
//            .each(new Fields("location_counts"), new AggreatorPrintFunction(),new Fields());
            
            .partitionBy(new Fields("location"))
            .partitionAggregate(new Fields("location"), new LocationAggregator(),new Fields("location_count"))
            .parallelismHint(2)
            .each(new Fields("location_count"),new AggreatorPrintFunction(),new Fields());

            Config conf = new Config();
            conf.setDebug(false);
            conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("log-process-topogie", conf, topology.build());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
