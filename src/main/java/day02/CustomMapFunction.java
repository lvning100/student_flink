package day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import util.ClickEvent;
import util.ClickSource;

/**
 * @Author: L.N
 * @Date: 2023/4/12 11:39
 * @Description: 自定义MapFunction
 */
public class CustomMapFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new ClickSource())
                .map(new MapFunction<ClickEvent, String>() {
                    @Override
                    public String map(ClickEvent in) throws Exception {
                        return in.username;
                    }
                })
                .print("使用匿明内部类实现");
        env.addSource(new ClickSource())
                .map(new MyMap())
                .print("使用外部类实现");
        env.addSource(new ClickSource())
                .map(r -> r.username)
                .print("使用匿名函数实现");
        env.addSource(new ClickSource())
                .flatMap(new FlatMapFunction<ClickEvent, String>() {
                    @Override
                    public void flatMap(ClickEvent in, Collector<String> out) throws Exception {
                        out.collect(in.username);
                    }
                })
                .print("使用FlatMap实现Map");
        env.fromElements(1,2,3)
                .map(r -> Tuple2.of(r, r))
                .returns(Types.TUPLE(Types.INT,Types.INT))
                .print("数据源是集合 + 使用lambda时使用非基本数据类型使用泛型擦除");
        env.execute();
    }
    // 每次输入的数据是一个ClickEvent类型的对象，输出是一个String对象
    public static class MyMap implements MapFunction<ClickEvent,String>{
        @Override
        public String map(ClickEvent in) throws Exception {
            return in.username;
        }
    }

}
