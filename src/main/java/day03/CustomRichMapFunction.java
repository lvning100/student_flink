package day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: L.N
 * @Date: 2023/4/13 16:29
 * @Description: 自定义富函数MapFunction 也就是添加了声明周期
 */
public class CustomRichMapFunction  {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(1,2,3)
               // .setParallelism(3)
                .map(new RichMapFunction<Integer, String>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("并行子任务索引：" + getRuntimeContext().getIndexOfThisSubtask()+", 声明周期开始");
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("并行子任务索引：" + getRuntimeContext().getIndexOfThisSubtask()+", 声明周期结束");
                    }

                    @Override
                    public String map(Integer value) throws Exception {
                        return "并行子任务索引 " + getRuntimeContext().getIndexOfThisSubtask() + ","
                                + "正在处理数据" + value;
                    }
                })
                .setParallelism(3)
                .print()
                .setParallelism(3);
        env.execute();
    }
}
