package com.zgg.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

// 批处理word count
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inpath = "src/main/resources/hello.txt";
        DataSet<String> inDataSet = env.readTextFile(inpath);
        // 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
        DataSet<Tuple2<String, Integer>> resDataSet = inDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)  // 按照第一个位置的word分组
                .sum(1);  // 将第二个位置上的数据求和

        resDataSet.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] wordArr = value.split(" ");
            for (String word:wordArr){
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
