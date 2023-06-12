package day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import util.ClickEvent;
import util.ClickSource;

/**
 * @Author: L.N
 * @Date: 2023/4/12 12:14
 * @Description: 自定义实现FilterFunction
 */
public class CustomFilterFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new ClickSource())
                .filter(new FilterFunction<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent in) throws Exception {
                        return in.username .equals("Mary") ;
                    }
                })
                .print("匿明内部类实现");

        env.addSource(new ClickSource())
                .filter(new MyFliter())
                .print("内部类实现");

        env.addSource(new ClickSource())
                .filter(r -> r.username.equals("Mary"))
                .print("lambda");
        
        env.addSource(new ClickSource())
                .flatMap(new FlatMapFunction<ClickEvent, ClickEvent>() {
                    @Override
                    public void flatMap(ClickEvent in, Collector<ClickEvent> out) throws Exception {
                        if (in.username.equals("Mary"))
                            out.collect(in);
                    }
                })
                .print("使用FlatMap实现");
        env.execute();
    }

    private static class MyFliter implements FilterFunction<ClickEvent> {
        @Override
        public boolean filter(ClickEvent in) throws Exception {
            return in.username.equals("Mary");
        }
    }
}
