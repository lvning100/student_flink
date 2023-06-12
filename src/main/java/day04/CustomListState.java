package day04;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import util.IntSource;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @Author: L.N
 * @Date: 2023/4/16 22:34
 * @Description:使用列表状态变量实现排序
 */
public class CustomListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntSource())
                .keyBy(r -> r % 2)
                .process(new SortOfHistory())
                .print("排序");
        env.execute();
    }

    public static class SortOfHistory extends KeyedProcessFunction<Integer,Integer,String>{
        private ListState<Integer> historyData;

        @Override
        public void open(Configuration parameters) throws Exception {
            //声明之后historyData状态变量中对应的hashMap表结构如下
            //  {
            //      0 : ArrayList[integer,...]
            //      1 : ArrayList[integer,...]
            //  }
            historyData = getRuntimeContext().getListState(new ListStateDescriptor<Integer>(
                    "history"
                    ,Types.INT));
        }

        @Override
        public void processElement(Integer in, Context ctx, Collector<String> out) throws Exception {
            //将in添加到historyData中对应key的ArrayList中
            historyData.add(in);
            // String 不能对数据进行修改
            // StringBuffer 能对数据进行修改 线程安全 速度慢 每次都是对StringBuffer本身修改
            // StringBuilder 能对数据进行修改 线程不安全 速度快 每次修改生成新的对象并改变对象引用

            StringBuffer result = new StringBuffer();
            if (ctx.getCurrentKey() == 0){
                result.append("偶数的历史数据排序结果是：");
            } else {
                result.append("奇数的历史数据排序结果是：");
            }

            //对历史数据排序
            //将列表状态变量中的数据都取出来放进integers数组中
            ArrayList<Integer> integers = new ArrayList<>();
            for (Integer integer : historyData.get()) {
                integers.add(integer);
            }
            //排序，使用比较器
            integers.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1 -o2;
                }
            });
            for (Integer i : integers) {
                result.append(i + " => ");
            }
            out.collect(result.toString());
        }
    }
}
