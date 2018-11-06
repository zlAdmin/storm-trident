package com.zl.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class PrintFunction extends BaseFunction{

    /**
     *Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = 1L;

    public void execute(TridentTuple tuple, TridentCollector collector) {
        // TODO Auto-generated method stub
        System.out.println(tuple.getString(0));
        System.out.println(tuple.getString(1));
        System.out.println(tuple.getString(2));
        collector.emit(tuple);
    }

}
