package com.atguigu.flink.tuning.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserInfo {

    private Integer id;
    private Long user_id;
    private Integer age;
    private Integer sex;
}
