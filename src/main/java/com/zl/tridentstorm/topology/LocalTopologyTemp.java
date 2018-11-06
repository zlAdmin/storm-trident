package com.zl.tridentstorm.topology;

import java.io.IOException;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;

import com.zl.trident.spout.FakeTweetsBatchSpout;

public class LocalTopologyTemp {
    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        try {
            FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(10);
            topology.newStream("spout", spout);

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
