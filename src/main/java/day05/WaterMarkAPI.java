package day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: L.N
 * @Date: 2023/4/21 17:14
 * @Description:在事件时间语义中添加水位线,
 * 根据数据源中的事件时间，每10s统计一次key的次数
 */
public class WaterMarkAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 9999)
                //设定输入的数据为（a,1000L）a是字符串 1000L 是事件时间
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String,Long> map(String in) throws Exception {
                        String[] array = in.split(" ");
                        return Tuple2.of(array[0],Long.parseLong(array[1])*1000L);
                    }
                })
                //添加水位线算子assignTimestampsAndWatermarks()
                //在map输出的数据流中插入水位线事件
                //默认是200ms插入一次
                .assignTimestampsAndWatermarks(
                        //设置水位线的策略：单调递增还是有界乱序
                        // 水位线 = 当前最大是事件时间 - 允许迟到的时间 - 1ms
                        //这里设置的是有界乱序，允许迟到时间是5s
                        WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        //设定数据中的事件时间字段
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element,long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String,
                        TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx,
                                        Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("Key:" + key + ",窗口" + ctx.window().getStart() + "~" + ctx.window().getEnd()+"里面有：" +elements.spliterator().getExactSizeIfKnown()+"条数据");
                    }
                })
                .print();
        env.execute();

    }
}
