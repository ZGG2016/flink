package com.atguigu.flink.tuning.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderInfo {

    Integer id;
    Long user_id;
    Double total_amount;
    Long create_time;

}
