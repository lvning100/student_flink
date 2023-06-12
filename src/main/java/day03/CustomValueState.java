package day03;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.Types;
import util.IntSource;
import util.Statistic;
import  org.apache.flink.api.common.state.ValueState;


/**
 * @Author: L.N
 * @Date: 2023/4/14 14:21
 * @Description:利用keyProcessFunction的值状态变量实现Reduce的功能
 *自定义reduce功能
 */
public class CustomValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new IntSource())
                .keyBy(r -> "int")
                .process( new MyReduce())
                .print();
        env.execute();
    }

    public static class MyReduce extends KeyedProcessFunction<String,Integer, Statistic>{
        //声明一个值状态变量
        private ValueState<Statistic> accumlator;
        @Override
        public void open(Configuration parameters) throws Exception {
            //对值状态变量进行初始化
            accumlator = getRuntimeContext().getState(
                    new ValueStateDescriptor<Statistic>("accmulator", Types.POJO(Statistic.class))
            );
        }

        @Override
        public void processElement(Integer in, Context ctx, Collector<Statistic> out) throws Exception {
            //第一条数据到来时，值状态变量为空
            //accumnlator.value() 获取的是输入数据的key所对应的状态变量
                if (accumlator.value() == null){
                    accumlator.update(new Statistic(in,in,in,in,1));
                } else{
                    Statistic oldAcc = accumlator.value();
                    Statistic newAcc = new Statistic(
                            Math.max(in, oldAcc.max),
                            Math.min(in, oldAcc.min),
                            (in + oldAcc.sum) / (oldAcc.count + 1),
                            oldAcc.sum + in,
                            oldAcc.count + 1
                    );
                    accumlator.update(newAcc);
                }

                out.collect(accumlator.value());
            }
        }
}
