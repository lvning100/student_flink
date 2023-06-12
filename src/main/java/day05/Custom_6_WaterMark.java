package day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.ProductViewCountPerWindow;
import util.UserBehavior;


import java.time.Duration;

/**
 * @Author: L.N
 * @Date: 2023/4/23 1:46
 * @Description: 读取csv数据源中的数据
 * csv是一个电商数据源
 * 每一件商品在每一次窗口中的浏览次数
 */
public class Custom_6_WaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\student_flink\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String in) throws Exception {
                        String[] array = in.split(",");
                        return new UserBehavior(
                                array[0],array[1],array[2],array[3],
                                Long.parseLong(array[4]) *1000L
                        );
                    }
                })
                //由于统计的是页面访问次数，因此只统计PV
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior in) throws Exception {
                        return in.type.equals("pv");
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element,
                                                         long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .keyBy(new KeySelector<UserBehavior, String>() {
                    @Override
                    public String getKey(UserBehavior in) throws Exception {
                        return in.productId;
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new CountAgg(),new WindowResult())
                .print();
        env.execute();
    }

    public static class CountAgg implements AggregateFunction<UserBehavior,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior in, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long,
            ProductViewCountPerWindow,String, TimeWindow>{
        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
            out.collect(new ProductViewCountPerWindow(key, elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()));
        }
    }
}
