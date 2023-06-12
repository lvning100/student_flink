package day03;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @Author: L.N
 * @Date: 2023/4/14 13:07
 * @Description: 使用keyProcessFunction实现注册定时器，执行定时器
 * 获取当前系统时间戳：ctx.timerService().currentProcessingTime();
 * 注册系统时间定时器：ctx.timerService().registerProcessingTimeTimer(系统时间戳);
 * 获取当前的key：ctx.getCurrentKey()
 * 使用时间戳需要new一个时间戳 ： new Timestamp(timestamp)
 */
public class CustomKeyProcessFunction2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .socketTextStream("hadoop102", 9999)
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return "socket";
                    }
                })
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        long currents = ctx.timerService().currentProcessingTime();
                        long thirtySecondsLater = currents + 30 * 1000L;
                        long sixtySecondsLater = currents + 60 * 1000L;
                        ctx.timerService().registerProcessingTimeTimer(thirtySecondsLater);
                        ctx.timerService().registerProcessingTimeTimer(sixtySecondsLater);
                        out.collect("当前的数据是:" + in +", key是:" + ctx.getCurrentKey() + "注册的定时器有："+
                                thirtySecondsLater +","+sixtySecondsLater);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("当前的key是:" + ctx.getCurrentKey() + "注册的定时器是:" + new Timestamp(timestamp)+
                                ",当前的执行时间是：" + ctx.timerService().currentProcessingTime());
                    }
                })
                .print("定时器的注册练习");
        env.execute();
    }
}
