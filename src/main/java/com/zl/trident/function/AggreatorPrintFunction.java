package com.zl.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class AggreatorPrintFunction extends BaseFunction{

    /**
     *Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = 1L;

    public void execute(TridentTuple tuple, TridentCollector collector) {
        System.out.println(tuple.getValue(0).toString());
        collector.emit(tuple);
    }

}
