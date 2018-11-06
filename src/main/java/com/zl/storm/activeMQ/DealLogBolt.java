package com.zl.storm.activeMQ;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.zl.utils.Dateutils;

public class DealLogBolt implements IBasicBolt{

    /**
     *Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = 4952633563634629135L;
    

    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
        // TODO Auto-generated method stub
        
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            System.out.println(input.getString(0));
            String [] spilts = input.getString(0).split("\t");
            String phone =spilts[0];
            String [] temp = spilts[1].split(",");
            String longitude = temp[0];
            String latitude = temp[1];
            long time = Dateutils.getInstance().getTime(spilts[2]);
            
            System.out.println(phone+";"+Double.parseDouble(longitude)+";"+Double.parseDouble(latitude)+";"+time);
            
            collector.emit(new Values(time,Double.parseDouble(longitude),Double.parseDouble(latitude)));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
    }

    public void cleanup() {
        // TODO Auto-generated method stub
        
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time","longitude","latitude"));
    }


}
