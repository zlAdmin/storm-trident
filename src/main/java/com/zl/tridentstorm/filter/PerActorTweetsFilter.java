package com.zl.tridentstorm.filter;

import java.util.Map;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

public class PerActorTweetsFilter extends BaseFilter{
    /**
     *Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = 1L;
    private int partitionIndex;
    
    String actor;

    public PerActorTweetsFilter(String actor) {
      this.actor = actor;
    }
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.partitionIndex = context.getPartitionIndex();
    }
    public boolean isKeep(TridentTuple tuple) {
        boolean filter = tuple.getString(0).equals(actor);
        if(filter) {
          System.err.println("I am partition [" + partitionIndex + "] and I have kept a tweet by: " + actor);
        }
        return filter;
        
    }

}
