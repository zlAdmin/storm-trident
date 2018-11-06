package com.zl.tridentstorm.def;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Values;

public class DiagnosisEventEmitter implements ITridentSpout.Emitter<Long>{

    public void emitBatch(TransactionAttempt tx, Long coordinatorMeta, TridentCollector collector) {
        for(int i=0 ; i<10;i++){
                List<Object> events = new ArrayList<Object>();
                double lat = new Double(-30+(int)(Math.random()*75 ));
                double lng = new Double(-120+(int)(Math.random()*70));
                long time = System.currentTimeMillis();
                String diag = new Integer(320+(int)(Math.random()*7)).toString();
                DiagnosisEvent event = new DiagnosisEvent(lat,lng,time,diag);
                events.add(event);
                //System.out.println(i);
                collector.emit(events);;
            }
    }

    public void success(TransactionAttempt tx) {
        // TODO Auto-generated method stub
        
    }

    public void close() {
        // TODO Auto-generated method stub
        
    }

}
