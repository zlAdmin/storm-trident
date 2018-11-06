package com.zl.hadoop.drpc;


import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 *   Rpc客户端   
 * @Filename: RpcClient.java
 * @Description: 
 * @Version: 1.0
 * @Author: 张磊
 * @Email: zhanglei@acmtc.com
 * @History:<br>
 *<li>Date: 2018年8月17日</li>
 *<li>Version: 1.0</li>
 *<li>Content: create</li>
 *
 */
public class RpcClient {
    public static void main(String[] args) throws IOException {
        
        Configuration conf= new Configuration();
        Long clientVersion = 88888L;
        UserService userService = RPC.getProxy(UserService.class, 
            clientVersion, new InetSocketAddress("localhost", 9999), conf);
        userService.addUser("zhangsan", "beijing");
        System.out.println("From Client Invock");
        RPC.stopProxy(userService);
        
    }

}
