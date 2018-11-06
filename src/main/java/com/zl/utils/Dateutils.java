package com.zl.utils;

import org.apache.commons.lang3.time.FastDateFormat;

public class Dateutils {
    public  Dateutils() {};
    
    private static Dateutils instance;
    FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    
    public static Dateutils getInstance() {
        if(instance == null) {
            instance = new Dateutils();
        }
        return instance;
    }
    public long getTime (String date) throws Exception{
        return format.parse(date.trim()).getTime();
    }
}
