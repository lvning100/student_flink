package day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @Author: L.N
 * @Date: 2023/4/13 17:02
 * @Description: 自定义并行数据源
 * 目的：数据源的并行度可以设置为2
 * RichParallelSourceFunction 是一个抽象类，里面的方法不用全部实现
 */
public class CustomParallelSourceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        for (int i = 0; i < 9; i++) {
                            ctx.collect(i);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .print("使用RichParallelSourceFunction实现并行度为2")
                .setParallelism(2);

//      自定义String类型的数据源 2个并行子任务都会执行run方法，因此数据是双份，但是每份数据源所在的并行子任务不同。
        env
                .addSource(new RichParallelSourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        for (int i = 0; i < 9; i++) {
                            ctx.collect("并行子任务索引:" + getRuntimeContext().getIndexOfThisSubtask() + ","
                                    + "正在处理数据" + i);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .print("同上使用String输出")
                .setParallelism(2);

        //虽然使用了并行数据源，但是仍然输出一份数据
        //rebalance 将每一个并行子任务的数据轮询发送到下游的所有并行子任务中
        //rescale 将每一个并行子任务的数据发送到下游的部分子任务中
        env
                .addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        for (int i = 0; i < 9; i++) {
                            if (i % 2 == getRuntimeContext().getIndexOfThisSubtask())
                                ctx.collect(i);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .rebalance()
                .print("输出一份数据")
                .setParallelism(4);
        env.execute();

    }
}
