package com.zl.trident.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;

import com.zl.tridentstorm.def.DefaultCoordinator;
import com.zl.tridentstorm.def.DiagnosisEventEmitter;

public class TridentSpoutTest implements ITridentSpout<Long>{

    /**
     *Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    
    
    //MasterBatchCoordinator会调用用户定义的BatchCoordinator的isReady()方法,如果返回true的话
    // 则会发送一个id为 batch的消息流，从而开始一个数据流转
    private static BatchCoordinator<Long> coordinatot = new DefaultCoordinator();
    //消息发送节点会接收协调spout的$batch和$success流。
    private static Emitter<Long> emitter = new DiagnosisEventEmitter();

    /**
     * Spout 的协调接口
     * @param txStateId
     * @param conf
     * @param context
     * @return
     * @see org.apache.storm.trident.spout.ITridentSpout#getCoordinator(java.lang.String, java.util.Map, org.apache.storm.task.TopologyContext)
     */
    public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf,
                                                   TopologyContext context) {
        return coordinatot;
    }

    /**
     * 发送消息的bolt接口
     * @param txStateId
     * @param conf
     * @param context
     * @return
     * @see org.apache.storm.trident.spout.ITridentSpout#getEmitter(java.lang.String, java.util.Map, org.apache.storm.task.TopologyContext)
     */
    public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
        
        return emitter;
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public Fields getOutputFields() {
        return new Fields("event");
    }
}
