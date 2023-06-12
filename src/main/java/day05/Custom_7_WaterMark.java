package day05;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: L.N
 * @Date: 2023/4/23 18:58
 * @Description:
 * 自定义水位线 ：在添加水位线出处自定义
 *
 * 添加数据源
 * 使用mapFunction对数据进行切分
 * 添加水位线
 * keyby 分组
 * window 开窗
 * ProcessWindowFunction 窗口计算
 * print 输出
 */
public class Custom_7_WaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadooop102", 9999)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] array = in.split(" ");
                        return Tuple2.of(
                                array[0],
                                Long.parseLong(array[1])*1000L
                        );
                    }
                })
                //添加水位线算子
                .assignTimestampsAndWatermarks(
                        new WatermarkStrategy<Tuple2<String, Long>>() {
                            @Override
                            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Tuple2<String, Long>>() {
                                    //设置水位线的延迟时间
                                    private long delay = 5000L;
                                    //为防止数据溢出，delay+1ms ,这就是观察到的最大事件时间
                                    private long maxTs = Long.MIN_VALUE + delay + 1L;
                                    //onEvent每来一条数据调用一次；
                                    @Override
                                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                                        long max = Math.max(event.f1, maxTs);
                                    }
                                    //周期性调用，默认每200ms调用一次
                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        output.emitWatermark(new Watermark(maxTs - delay -1L));
                                    }
                                };
                            }

                            @Override
                            public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                                return new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element,
                                                                 long recordTimestamp) {
                                        return element.f1;
                                    }
                                };
                            }
                        }
                )
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> in) throws Exception {
                        return in.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String,
                        TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String,
                            Long>> elements, Collector<String> out) throws Exception {
                        out.collect("Key:" + key +",窗口:" +context.window().getStart()+"~"+context.window().getEnd()+"里面有"+elements.spliterator().getExactSizeIfKnown()+"条数据");
                    }
                })
                .print();
        env.execute();

    }
}
