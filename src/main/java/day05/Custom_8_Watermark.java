package day05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

/**
 * @Author: L.N
 * @Date: 2023/4/23 19:53
 * @Description:自定义水位线
 * 数据源中定义水位线
 */
public class Custom_8_Watermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        env
                .addSource(new SourceFunction<String>() {
                    //run函数执行之前，FLink会发送一条-Max的水位线
                    //run函数执行之后，FLink会发送一条+Max的水位线
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.emitWatermark(new Watermark(-10000L));
                        ctx.collectWithTimestamp("hello", 1000L);
                        ctx.emitWatermark(new Watermark(10000L));
                    }
                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> r)
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() +1000L);
                        out.collect("key:" + ctx.getCurrentKey() + "，输入数据：" + in + "，事件时间：" +
                                "" + ctx.timestamp() + ",注册的定时器时间戳：" + (ctx.timestamp() + 1000L) +
                                "，当前process的水位线：" + ctx.timerService().currentWatermark());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("key:"+ctx.getCurrentKey()+"定时器时间戳是:"+timestamp+"当前process"
                                + "的水位线的时间是"+ ctx.timerService().currentWatermark());
                    }
                })
                .print();
        env.execute();
    }
}
