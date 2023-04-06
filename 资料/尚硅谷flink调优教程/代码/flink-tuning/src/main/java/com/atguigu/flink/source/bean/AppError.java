package com.atguigu.flink.source.bean;

import com.atguigu.flink.source.util.RandomNum;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.net.ConnectException;

@Data
@AllArgsConstructor
public class AppError {

    Integer error_code;

    String msg;

    public static  AppError build(){
        int errorCode = RandomNum.getRandInt(1001, 4001);
        String msg=" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.log.bean.AppError.main(AppError.java:xxxxxx)";
        return new AppError(errorCode,msg);

    }



    public static void main(String[] args) throws  Exception {
        throw  new ConnectException() ;
    }


  //


}
