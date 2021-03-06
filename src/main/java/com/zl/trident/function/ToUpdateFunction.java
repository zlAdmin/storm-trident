package com.zl.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class ToUpdateFunction extends BaseFunction{

    /**
     *Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = -1889457425120557773L;

    public void execute(TridentTuple tuple, TridentCollector collector) {
        collector.emit(new Values(tuple.getString(0).toUpperCase()));
        
    }

}
