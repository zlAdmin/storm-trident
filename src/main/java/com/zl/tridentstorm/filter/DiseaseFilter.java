package com.zl.tridentstorm.filter;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zl.tridentstorm.def.DiagnosisEvent;

public class DiseaseFilter extends BaseFilter{
    
    /**
     *Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = -3414066586146658290L;
    
    
    private static final Logger LOG = LoggerFactory.getLogger(DiseaseFilter.class);

    public boolean isKeep(TridentTuple tuple) {
        DiagnosisEvent diagnosis = (DiagnosisEvent)tuple.getValue(0);
        Integer code = Integer.parseInt(diagnosis.diagnosisCode);
        //过滤出 code小于322的疾病
        if(code.intValue() <=322){
            LOG.debug("Emitting disease ["+diagnosis.diagnosisCode+"]");
            return true;
        }else{
            LOG.debug("Filtering disease ["+diagnosis.diagnosisCode+"]");
            return false;
        }
    }

}
