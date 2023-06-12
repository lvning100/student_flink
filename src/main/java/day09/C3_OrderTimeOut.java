package day09;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.Event;

import java.util.List;
import java.util.Map;

/**
 * @Author: L.N
 * @Date: 2023/5/4 8:44
 * @Description: 订单超时检测
 */
public class C3_OrderTimeOut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<Event, String> stream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("order-1", "create-order", 1000L),
                                1000L);
                        ctx.collectWithTimestamp(new Event("order-2", "create-order", 2000L),
                                2000L);
                        ctx.collectWithTimestamp(new Event("order-1", "pay-order", 3000L), 3000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> r.key);

        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("create")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event in) throws Exception {
                        return in.value.equals("create-order");
                    }
                })
                .next("pay")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event in) throws Exception {
                        return in.value.equals("pay-order");
                    }
                })
                //要求两个事件在5秒钟内发生
                .within(Time.seconds(5));

        //在数据流中应用模板对数据进行检索
        SingleOutputStreamOperator<String> result = CEP
                .pattern(stream, pattern)
                .flatSelect(
                        //侧输出流标签，用来接收超时信息
                        new OutputTag<String>("timeout-info") {
                        },
                        //匿名类，用来发送超时信息
                        new PatternFlatTimeoutFunction<Event, String>() {
                            @Override
                            public void timeout(Map<String, List<Event>> map, long l,
                                                Collector<String> collector) throws Exception {
                                //  map {
                                //      "create" : [Event]
                                //  }
                                Event create = map.get("create").get(0);
                                //发送超时信息到侧输出流
                                collector.collect(create.key + "超时未支付");
                            }
                        },
                        //匿名类，用来发送匹配上的信息
                        new PatternFlatSelectFunction<Event, String>() {
                            @Override
                            public void flatSelect(Map<String, List<Event>> map,
                                                   Collector<String> collector) throws Exception {
                                //  {
                                //      "create" : [Event]
                                //      "pay" : [Event]
                                //  }
                                Event create = map.get("create").get(0);
                                Event pay = map.get("pay").get(0);
                                collector.collect(create.key + "在" + pay.ts + "完成支付.");
                            }
                        }
                );
        result.print("main");
        result.getSideOutput(new OutputTag<String>("timeout-info"){}).print("side");
        env.execute();
    }
}
