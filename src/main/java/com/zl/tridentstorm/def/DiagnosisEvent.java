package com.zl.tridentstorm.def;

import java.io.Serializable;

public class DiagnosisEvent implements Serializable {
    
    
    /**
     *Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = -3216731133123051481L;
    public double lat ;
    public double lng ;
    public long time ;
    public String diagnosisCode ;

    public DiagnosisEvent(double lat ,double lng ,long time, String diagnosisCode){
        super();
        this.time=time;
        this.lat = lat ;
        this.lng = lng ;
        this.diagnosisCode = diagnosisCode;
    }
}