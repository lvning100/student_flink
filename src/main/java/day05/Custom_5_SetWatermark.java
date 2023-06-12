package day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: L.N
 * @Date: 2023/4/23 1:09
 * @Description:根据数据源中的事件时间，每10s统计一次key的次数
 *使用默认的水位线生成策略
 * 使用keyProcessFunction开窗
 */
public class Custom_5_SetWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] array = in.split(" ");
                        return Tuple2.of(
                                array[0],
                                Long.parseLong(array[1]) * 1000L);
                    }
                })
                //添加水位线的算子
                .assignTimestampsAndWatermarks(
                        //指定水位线生成策略，并指定延迟时间
                        WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                //指定数据源中的字段为是事件时间
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String,Long> element,
                                                         long recordTimestamp) {
                                return element.f1;
                            }
                        })
                )
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> in, Context ctx,
                                               Collector<String> out) throws Exception {
                        //根据事件时间注册定时器
                        ctx.timerService().registerEventTimeTimer(in.f1 + 9999L);
                        out.collect("Key是:"+ctx.getCurrentKey()+"当前key是"+in.f0+",当前process水位线是"+ctx.timerService().currentWatermark()+",当前事件时间的时间戳是"+in.f1+"注册的定时器是:" + (in.f1+9999L));
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("Key:"+ctx.getCurrentKey()+"定时器时间戳是:"+timestamp + "当前process"
                                + "的水位线是"+ctx.timerService().currentWatermark());
                    }
                })
                .print();
        env.execute();
    }
}
