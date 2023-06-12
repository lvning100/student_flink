package day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.UserBehavior;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @Author: L.N
 * @Date: 2023/4/27 22:48
 * @Description:独立访客
 * 计算每小时有多少个访客
 */
public class Custom_5_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\student_flink\\src\\main\\resources\\UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String in, Collector<UserBehavior> out) throws Exception {
                        String[] array = in.split(",");
                        UserBehavior userBehavior = new UserBehavior(
                                array[0],
                                array[1],
                                array[2],
                                array[3],
                                Long.parseLong(array[4]) * 1000L
                        );

                        if (userBehavior.type.equals("pv")){
                            out.collect(userBehavior);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element,
                                                         long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .keyBy( r -> "userbehavior")
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(
                        new AggregateFunction<UserBehavior, HashSet<String>, Long>() {
                            @Override
                            public HashSet<String> createAccumulator() {
                                return new HashSet<>();
                            }

                            @Override
                            public HashSet<String> add(UserBehavior value,
                                                       HashSet<String> accumulator) {
                                accumulator.add(value.userId);
                                return accumulator;
                            }

                            @Override
                            public Long getResult(HashSet<String> accumulator) {
                                return (long) accumulator.size();
                            }

                            @Override
                            public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
                                return null;
                            }
                        }
                        , new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                            @Override
                            public void process(String string, Context context,
                                                Iterable<Long> elements, Collector<String> out) throws Exception {
                                out.collect("窗口" + new Timestamp(context.window().getStart()) +
                                        "~" + new Timestamp(context.window().getEnd()) + "的uv是："+elements.iterator().next());
                            }
                        })
                .print();
        env.execute();

    }
}
