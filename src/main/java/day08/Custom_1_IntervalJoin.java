package day08;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.Event;

/**
 * @Author: L.N
 * @Date: 2023/4/29 1:41
 * @Description: 基于窗口的join
 */
public class Custom_1_IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> left = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "left", 10* 1000L), 10 * 1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });
        DataStreamSource<Event> right = env.addSource(new SourceFunction<Event>() {
            @Override
            public void run(SourceContext<Event> ctx) throws Exception {
                ctx.collectWithTimestamp(new Event("key-1", "right", 2 * 1000L), 2 * 1000L);
                ctx.collectWithTimestamp(new Event("key-1", "right", 8 * 1000L), 8 * 1000L);
                ctx.collectWithTimestamp(new Event("key-1", "right", 12 * 1000L), 12 * 1000L);
                ctx.collectWithTimestamp(new Event("key-1", "right", 22 * 1000L), 22 * 1000L);
            }

            @Override
            public void cancel() {}
        });
        left
                .keyBy(r ->r.key)
                .intervalJoin(right.keyBy(r->r.key))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<Event, Event, String>() {
                    @Override
                    public void processElement(Event left, Event right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + "=>" +right);
                    }
                })
                .print();
        env.execute();
    }
}
