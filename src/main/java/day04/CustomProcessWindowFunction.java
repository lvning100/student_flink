package day04;

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
 * @Date: 2023/4/17 15:52
 * @Description: 计算每个用户在没5秒钟的访问次数
 * 此方法在窗口中维护了当前用户浏览的所有页面，对我们只要一个次数来说，比较浪费，可以结合Aggregate进行优化
 */
public class CustomProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                //设置5秒钟的处理时间滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new WindowResult())
                .print();
        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<ClickEvent,UserViewCountPerWindow,String, TimeWindow>{
        @Override
        public void process(String key, Context context, Iterable<ClickEvent> elements,
                            Collector<UserViewCountPerWindow> out) throws Exception {
            //Iterable<ClickEvent> elements 包含了窗口的所有元素
            //elements.spliterator().getExactSizeIfKnown() 获取迭代器中的元素数量
            out.collect(new UserViewCountPerWindow(key,
                    elements.spliterator().getExactSizeIfKnown(), context.window().getStart(),
                    context.window().getEnd()));
        }
    }
}
