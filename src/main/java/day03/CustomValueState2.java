package day03;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import util.IntSource;
import util.Statistic;

/**
 * @Author: L.N
 * @Date: 2023/4/16 20:07
 * @Description:利用keyProcessFunction的值状态变量实现Reduce的功能 并且设置每10s触发一次，
 */
public class CustomValueState2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntSource())
                .keyBy(r -> "int")
                .process(new MyReduce2())
                .print("10s截流");
        env.execute();
    }

    public static class MyReduce2 extends KeyedProcessFunction<String,Integer, Statistic>{
        //声明值状态变量
        private ValueState<Statistic> accumlator;
        //声明标志位，用来指示是否存在定时器
        private ValueState<Integer> flag;
        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化值状态变量
            accumlator = getRuntimeContext().getState(new ValueStateDescriptor<Statistic>(
                    "accumlator", Types.POJO(Statistic.class)));
            flag = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("flag",
                    Types.INT));
        }

        @Override
        public void processElement(Integer in, Context ctx, Collector<Statistic> out) throws Exception {
            //当第一条数据到来时，对值状态变量进行初始化并更新
            //当非第一条数据到来时，对值状态变量进行更新
            //accumlator.value() 访问的是输入数据的key多对应的状态变量
            //ctx.getCurrentKey()
            if (accumlator.value() == null){
                accumlator.update(new Statistic(in,in,in,in,1));
            } else{
                Statistic oldAcc = accumlator.value();
                Statistic newAcc = new Statistic(
                        Math.max(in, oldAcc.max),
                        Math.min(in, oldAcc.min),
                        (oldAcc.sum + in) / (oldAcc.count + 1),
                        oldAcc.sum + in,
                        oldAcc.count + 1
                );
                //更新当前key所对应的状态变量
                accumlator.update(newAcc);
            }
            //如果当前没有定时器则注册一个定时器，有定时器则不管，数据的向下传送在onTimer()
            if (flag.value() == null){
                ctx.timerService().registerProcessingTimeTimer(
                        ctx.timerService().currentProcessingTime() + 10 * 1000L
                );
                flag.update(1);
                }
            }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Statistic> out) throws Exception {
            //如果触发了定时器，则立马向下传输数据
            out.collect(accumlator.value());
            //清除定时器
            flag.clear();

        }
    }
}
