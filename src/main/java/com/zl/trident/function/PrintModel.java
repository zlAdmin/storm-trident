package com.zl.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import com.zl.tridentstorm.def.DiagnosisEvent;

public class PrintModel extends BaseFunction{

    /**
     *Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = -3111100613861296447L;

    public void execute(TridentTuple tuple, TridentCollector collector) {
        DiagnosisEvent diagnosis = (DiagnosisEvent)tuple.getValue(0);
        String diagnosisCode = diagnosis.diagnosisCode;
        System.out.println("hello"+diagnosisCode);
    }

}
