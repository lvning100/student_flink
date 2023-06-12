package day06;

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
 * @Date: 2023/4/24 14:42
 * @Description: 水位线传播分流
 * 一对多的情况
 * 添加数据源，并行度为1
 * 添加水位线 ：无法指明并行度
 * keyBy ：无法指定并行度
 * 开窗：指定并行度4
 * 输出：指定并行度4
 */
public class Custom_3_WaterMarkSpread {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] array = in.split(" ");
                        return Tuple2.of(
                                array[0],
                                Long.parseLong(array[1]) * 1000L
                        );
                    }
                })
                .setParallelism(1)
                //添加水位线算子
                .assignTimestampsAndWatermarks(
                        //指定水位线策略，选择有界乱序并指定最大延迟时间是3s
                        WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,
                                Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element,
                                                         long recordTimestamp) {
                                return element.f1;
                            }
                        })
                )
                .setParallelism(1)
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String,
                        TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx, Iterable<Tuple2<String,
                            Long>> elements, Collector<String> out) throws Exception {
                        out.collect("Key:"+key+",窗口" + ctx.window().getStart()+"~"+ctx.window().getEnd()+ "中有"+elements.spliterator().getExactSizeIfKnown()+"条数据,并行子任务是"+getRuntimeContext().getIndexOfThisSubtask());
                    }
                })
                .setParallelism(4)
                .print()
                .setParallelism(4);
        env.execute();
    }
}
