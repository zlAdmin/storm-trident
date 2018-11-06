package com.zl.hadoop.drpc;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.shade.org.json.simple.JSONObject;



public class Tes {
    public static void main(String[] args) {
        Date date =new Date();
        SimpleDateFormat simple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String da = simple.format(date);
        String MM= da.substring(5, 7);
        String dd= da.substring(8, 10);
        String HH= da.substring(11, 13);
        String mm= da.substring(14, 16);
        StringBuilder build = new StringBuilder();
        build.append(MM).append("月");
        build.append(dd).append("日");
        build.append(HH).append("时");
        build.append(mm).append("分");
        System.out.println(build.toString());
        
        Map map =new HashMap();
        map.put("customer", "gaoxiang");
        map.put("phoneNumber", "test");
        String id = JSONObject.toJSONString(map);
        //"{\"customer\":\"gaoxiang\", \"phoneNumber\":\"test\", \"date\":\"8月21日\", \"orderName\":\"产品订单\"}"
        System.out.println(id);
        
    }

}
