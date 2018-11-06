package com.zl.tridentstorm.topology;

import java.io.IOException;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

import com.zl.trident.function.AggreatorPrintFunction;
import com.zl.trident.spout.FakeTweetsBatchSpout;

public class GroupByTopology {
    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        try {
            FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(10);
            topology.newStream("spout", spout)
            .groupBy(new Fields("location"))
            .aggregate(new Fields("location"), new Count(), new Fields("count"))
            .each(new Fields("location","count"), new AggreatorPrintFunction(),new Fields());

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
