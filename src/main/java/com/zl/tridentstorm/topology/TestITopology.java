package com.zl.tridentstorm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

import com.zl.trident.function.PrintModel;
import com.zl.trident.spout.TridentSpoutTest;

public class TestITopology {
    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        TridentSpoutTest spout = new TridentSpoutTest();
        topology.newStream("spout", spout)
        .each(new Fields("event"), new PrintModel(),new Fields());
        

        Config conf = new Config();
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("log-process-topogie", conf, topology.build());
    }

}
