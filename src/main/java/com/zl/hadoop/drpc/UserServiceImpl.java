package com.zl.hadoop.drpc;

public class UserServiceImpl implements UserService{

    public void addUser(String name, String country) {
       System.out.println("client--新增用户信息，姓名："+name+"城市："+country);
        
    }

}
