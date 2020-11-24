package com.oudongyu.bigdata;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class FlinkTransformations {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = (LocalStreamEnvironment) StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> value1 = env.fromElements(new Tuple2<>("a", 1), new Tuple2<>("b", 2));
        DataStreamSource<Integer> value2 = env.fromElements(1, 2, 3);
        value1.connect(value2).map(new CoMapFunction<Tuple2<String, Integer>, Integer, Integer>() {
            @Override
            public Integer map1(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f1;
            }

            @Override
            public Integer map2(Integer integer) throws Exception {
                return integer;
            }
        }).map(x->x*100).print();
        env.execute();
    }
}
