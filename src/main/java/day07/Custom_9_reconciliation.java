package day07;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import util.Event;

import java.awt.*;

/**
 * @Author: L.N
 * @Date: 2023/4/28 17:22
 * @Description: 实时对账
 */
public class Custom_9_reconciliation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream1 = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "left", 1000L), 1000L);
                        ctx.collectWithTimestamp(new Event("key-2", "left", 2000L), 2000L);
                }
                    @Override
                    public void cancel() {
                    }
                });

        DataStreamSource<Event> stream2 = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "right", 3000L), 3000L);
                        ctx.collectWithTimestamp(new Event("key-3", "right", 4000L), 4000L);
                        ctx.collectWithTimestamp(new Event("key-2", "right", 1000*1000L), 1000*1000L);
                    }

                    @Override
                    public void cancel() {}
                });

        stream1
                .keyBy(r -> r.key)
                .connect(stream2.keyBy(r -> r.key))
                .process(new Match())
                .print();
        env.execute();
    }

    public static class Match extends CoProcessFunction<Event,Event,String>{
        private ValueState<Event> leftState;
        private ValueState<Event> rightState;

        @Override
        public void open(Configuration parameters) throws Exception {
            leftState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Event>("leftState", Types.POJO(Event.class))
            );
            rightState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Event>("rightState", Types.POJO(Event.class))
            );
        }

        @Override
        public void processElement1(Event in1, Context ctx, Collector<String> out) throws Exception {
            if (rightState.value() == null){
                // 说明left事件先到达
                leftState.update(in1);
                ctx.timerService().registerEventTimeTimer(in1.ts + 5000L);
            } else{
                out.collect(in1.key + "对账成功，right事件先到达");
                //对账成功，清空状态列表
                rightState.clear();
            }
        }

        @Override
        public void processElement2(Event in2, Context ctx, Collector<String> out) throws Exception {
            if (leftState.value() == null){
                //说明右边的数据先到达
                rightState.update(in2);
                ctx.timerService().registerEventTimeTimer(in2.ts + 5000L);
            } else{
                out.collect(in2.key + "对账成功，左边的数据先到达");
                leftState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if (leftState.value() != null){
                out.collect(leftState.value().key + "对账失败，右边的数据没有到达");
                leftState.clear();
            }
            if (rightState.value() != null){
                out.collect(rightState.value().key + "对账失败，左边的数据没有到达");
                rightState.clear();
            }
        }
    }
}
