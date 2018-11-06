package com.zl.tridentstorm.aggregator;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.collections.MapUtils;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class LocationAggregator extends BaseAggregator<Map<String,Integer>>{

    public Map<String, Integer> init(Object batchId, TridentCollector collector) {
        return new HashMap<String, Integer>();
    }

    public void aggregate(Map<String, Integer> val, TridentTuple tuple,
                          TridentCollector collector) {
        String location = tuple.getString(0);
        val.put(location, MapUtils.getInteger(val, location, 0) + 1);
        
    }

    public void complete(Map<String, Integer> val, TridentCollector collector) {
        collector.emit(new Values(val));
    }

}
