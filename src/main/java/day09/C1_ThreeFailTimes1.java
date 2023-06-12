package day09;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import util.Event;

import java.util.List;
import java.util.Map;

/**
 * @Author: L.N
 * @Date: 2023/5/3 23:09
 * @Description: 使用Flink-cep检测连续三次登录失败
 * 重复模板单个写：.next
 * 挑选模板匹配字段输出 PatternSelectFunction
 */
public class C1_ThreeFailTimes1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //定义数据源
        DataStreamSource<Event> stream = env.addSource(new SourceFunction<Event>() {
            @Override
            public void run(SourceContext<Event> ctx) throws Exception {
                ctx.collectWithTimestamp(new Event("user-1", "fail", 1000L), 1000L);
                ctx.collectWithTimestamp(new Event("user-1", "fail", 2000L), 2000L);
                ctx.collectWithTimestamp(new Event("user-2", "success", 3000L), 3000L);
                ctx.collectWithTimestamp(new Event("user-1", "fail", 4000L), 4000L);
                ctx.collectWithTimestamp(new Event("user-1", "fail", 5000L), 5000L);
            }

            @Override
            public void cancel() {

            }
        });
        //定义模板
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("first")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event in) throws Exception {
                        return in.value.equals("fail");
                    }
                })
                //表示紧邻first的事件
                .next("second")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event in) throws Exception {
                        return in.value.equals("fail");
                    }
                })
                //第三次事件
                .next("third")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event in) throws Exception {
                        return in.value.equals("fail");
                    }
                });

        //使用模板在数据流上检测连续三次登录失败
        CEP
                .pattern(stream.keyBy(r -> r.key), pattern)
                //从匹配的数据中挑选字段
                .select(new PatternSelectFunction<Event, String>() {
                    @Override
                    public String select(Map<String, List<Event>> map) throws Exception {
                        // List列表中的数据结构是
                        // {
                        //      "first" : [Event]
                        //      "second" : [Event]
                        //       "third" : [Event]
                        // }
                        Event first = map.get("first").get(0);
                        Event second = map.get("second").get(0);
                        Event third = map.get("third").get(0);
                        return first.key +"连续三次登录失败，时间戳是"+first.ts+";"+second.ts+";"+third.ts+";";
                    }
                })
                .print();
        env.execute();
    }
}
