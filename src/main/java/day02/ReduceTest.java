package day02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: L.N
 * @Date: 2023/4/12 23:04
 * @Description: 验证keyby的路由方式，三个key，4个reduce
 */
public class ReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .keyBy(r -> r % 3)
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer acc, Integer value2) throws Exception {
                        return acc+=value2;
                    }
                })
                .setParallelism(4)
                .print()
                .setParallelism(4);
        env.execute();
    }
}
