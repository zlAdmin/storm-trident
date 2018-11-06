package com.zl.tridentstorm.topology;

import java.io.IOException;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

import com.zl.trident.function.PrintFunction;
import com.zl.trident.function.ToUpdateFunction;
import com.zl.trident.spout.FakeTweetsBatchSpout;
import com.zl.tridentstorm.filter.PerActorTweetsFilter;

public class FiltreTestTopology {
    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        try {
            FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(10);
            topology.newStream("spout", spout)
            .parallelismHint(2)
            .partitionBy(new Fields("actor"))
            .each(new Fields("actor", "text"), new PerActorTweetsFilter("dave"))
            .parallelismHint(5)//设置并行度
            .each(new Fields("text"), new ToUpdateFunction(),new Fields("up_text"))
            .project(new Fields("actor","text","up_text"))
            .each(new Fields("actor", "text","up_text"), new PrintFunction(), new Fields());
            
            
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
