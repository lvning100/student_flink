package day06;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author: L.N
 * @Date: 2023/4/24 20:31
 * @Description: 将迟到数据输出到侧输出流
 * 使用侧输出流
 * 数据源定义三个数据 + 1个水位线
 *
 */
public class Custom_5_OutputTag {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<String> result = env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.collectWithTimestamp("a", 1000L);
                        ctx.collectWithTimestamp("a", 2000L);
                        ctx.emitWatermark(new Watermark(1999L));
                        ctx.collectWithTimestamp("a", 1500L);
                    }

                    @Override
                    public void cancel() {
                    }
                })
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        //判断当前数据的事件时间是否小于当前水位线，小于迟到侧输出流，大于不迟到
                        if (ctx.timestamp() < ctx.timerService().currentWatermark()) {
                            //使用output侧输出流算子
                            ctx.output(
                                    new OutputTag<String>("late-event"){},
                                    //向侧输出流中发送数据
                                    "数据" + in + "," + ctx.timestamp() + "迟到了"
                            );
                        } else {
                            out.collect("数据" + in + " , " + ctx.timestamp() + "没有迟到");
                        }
                    }
                });
        result.print("主流");
        result.getSideOutput(new OutputTag<String>("late-event"){}).print("侧输出流");
        env.execute();
    }
}
