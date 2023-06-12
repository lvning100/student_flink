package day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: L.N
 * @Date: 2023/4/27 15:14
 * @Description: 使用触发器
 * 窗口的有界乱序时间为0s 设置为单调递增
 * 设置滚动窗口10s
 * 每来一条数据触发一次
 */
public class Custom_3_Trigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String in, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] array = in.split(" ");
                        out.collect(Tuple2.of(
                                array[0],
                                Long.parseLong(array[1]) * 1000L
                        ));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,
                                        Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element,
                                                                 long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new Trigger<Tuple2<String, Long>, TimeWindow>() {
                    //每来一次数据触发一次
                    @Override
                    public TriggerResult onElement(Tuple2<String, Long> element, long timestamp,
                                                   TimeWindow window, TriggerContext ctx) throws Exception {
                        //触发窗口的计算
                        return TriggerResult.FIRE;
                    }

                    //处理时间定时器
                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window,
                                                          TriggerContext ctx) throws Exception {
                        return null;
                    }

                    //事件时间定时器：
                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window,
                                                     TriggerContext ctx) throws Exception {
                        if (time == window.getEnd() - 1L) {
                            return TriggerResult.FIRE_AND_PURGE;//触发窗口计算并清空窗口
                        } else
                            return TriggerResult.CONTINUE;//什么都不做
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                    }
                })
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String,
                        TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx, Iterable<Tuple2<String,
                            Long>> elements, Collector<String> out) throws Exception {
                        out.collect("Key:" + key + "窗口是:" + ctx.window().getStart() + "~" + ctx.window().getEnd() + "窗口中有" + elements.spliterator().getExactSizeIfKnown() + "条数据");
                    }
                })
                .print();
        env.execute();
    }
}
