package day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author: L.N
 * @Date: 2023/4/25 16:09
 * @Description: 将迟到的数据放入侧输出流
 * .sideOutputLateData(new OutputTag<Tuple2<String, Long>>("late-event") {} 窗口之后的迟到数据使用它放到侧输出流
 */
public class Custom_6_OutPutTag {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                // a  1
                // a  2
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] array = in.split(" ");
                        return Tuple2.of(
                                array[0],
                                Long.parseLong(array[1]) * 1000L
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
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
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(
                        new OutputTag<Tuple2<String, Long>>("late-event") {
                        }
                )
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String,
                        TimeWindow>() {
                    @Override
                    public void process(String key, Context context,
                                        Iterable<Tuple2<String, Long>> elements,
                                        Collector<String> out) throws Exception {
                        out.collect("key:" + key + "，窗口:" + context.window().getStart() + "~" + context.window().getEnd() + ",里面有 " + elements.spliterator().getExactSizeIfKnown() + "条数据");

                    }
                });
        result.print("main");
        result.getSideOutput(new OutputTag<Tuple2<String,Long>>("late-event"){}).print("side");
        env.execute();
    }
}
