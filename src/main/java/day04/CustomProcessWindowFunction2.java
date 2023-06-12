package day04;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.util.Collector;
import util.ClickEvent;
import util.ClickSource;
import util.UserViewCountPerWindow;

/**
 * @Author: L.N
 * @Date: 2023/4/17 16:10
 * @Description:计算每个用户在没5秒钟的访问次数 使用ProcessWindowFunction +AggregateFunction实现
 */
public class CustomProcessWindowFunction2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg (), new WindowResult())
                .print();
        env.execute();
    }

    public static class CountAgg implements AggregateFunction<ClickEvent,Long,Long>{
        @Override
        public Long createAccumulator() {
            return 100L;
        }

        @Override
        public Long add(ClickEvent value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, UserViewCountPerWindow,
            String, TimeWindow>{
        @Override
        public void process(String key, Context ctx, Iterable<Long> elements,
                            Collector<UserViewCountPerWindow> out) throws Exception {
            out.collect(new UserViewCountPerWindow(key,elements.iterator().next(),
                    ctx.window().getStart(),ctx.window().getEnd()));
        }
    }
}
