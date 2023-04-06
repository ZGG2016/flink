package com.atguigu.flink.tuning;

import com.atguigu.flink.tuning.bean.OrderInfo;
import com.atguigu.flink.tuning.bean.UserInfo;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;


public class DataStreamDataGenDemo {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.ENABLE_FLAMEGRAPH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(1);
        env.disableOperatorChaining();


        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env
                .addSource(new DataGeneratorSource<OrderInfo>(
                        new RandomGenerator<OrderInfo>() {
                            @Override
                            public OrderInfo next() {
                                return new OrderInfo(
                                        random.nextInt(1, 100000),
                                        random.nextLong(1, 1000000),
                                        random.nextUniform(1, 1000),
                                        System.currentTimeMillis());
                            }
                        }
                ))
                .returns(Types.POJO(OrderInfo.class));


        SingleOutputStreamOperator<UserInfo> userInfoDS = env
                .addSource(new DataGeneratorSource<UserInfo>(
                        new SequenceGenerator<UserInfo>(1, 1000000) {
                            RandomDataGenerator random = new RandomDataGenerator();

                            @Override
                            public UserInfo next() {
                                return new UserInfo(
                                        valuesToEmit.peek().intValue(),
                                        valuesToEmit.poll().longValue(),
                                        random.nextInt(1, 100),
                                        random.nextInt(0, 1));
                            }
                        }
                ))
                .returns(Types.POJO(UserInfo.class));

        orderInfoDS.print("order>>");
        userInfoDS.print("user>>");


        env.execute();
    }
}
