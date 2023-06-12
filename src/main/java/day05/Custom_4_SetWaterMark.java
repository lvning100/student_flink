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

import java.sql.Timestamp;
import java.time.Duration;


/**
 * @Author: L.N
 * @Date: 2023/4/22 21:51
 * @Description:根据数据源中的事件时间，每10s统计一次key的次数
 * 不使用默认的200ms产生一次水位线，而是自己设置水位线1分钟
 * 使用窗口开窗 ，之后使用keyProcessFunction开窗
 */
public class Custom_4_SetWaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置每1分钟插入一次水位线
        env.getConfig().setAutoWatermarkInterval(60 * 1000L);

        env
                //数据源是(a,1)
                .socketTextStream("hadoop102", 9999)
                //
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] array = in.split(" ");
                        return Tuple2.of(
                                array[0],
                                //将时间戳s装换成ms
                                Long.parseLong(array[1])*1000L
                        );
                    }
                })
                //使用添加水位线算子
                .assignTimestampsAndWatermarks(
                        //指定水位线的生成策略,并设置水位线的延迟时间是10s
                        WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                //指定数据中的字段为事件时间
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })

                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String,TimeWindow>() {
                    @Override
                    public void process(String key, Context context,Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {

                        out.collect("Key是:"+key +"，窗口开始时间是:"+context.window().getStart()+"~"+context.window().getEnd()+"有"+elements.spliterator().getExactSizeIfKnown()+"条数据。");
                    }
                })
                .print("10s计算一次key中的数据条数");
        env.execute();

    }
}
