package com.zl.hadoop.drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class RpcService {
    public static void main(String[] args) throws Exception{
        
        Configuration conf =new Configuration();
        RPC.Builder builder = new RPC.Builder(conf);
        RPC.Server service = builder.setProtocol(UserService.class)
        .setInstance(new UserServiceImpl())
        .setBindAddress("10.8.3.25")
        .setPort(9999)
        .build();
        
        service.start();
    }
}
