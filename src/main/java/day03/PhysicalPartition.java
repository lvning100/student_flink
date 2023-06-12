package day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: L.N
 * @Date: 2023/4/13 14:52
 * @Description:物理分区算子
 */
public class PhysicalPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(1,2,3,4,5,6,7,8,9)
                .setParallelism(1)
                .shuffle()
                .print("shuffle 随机分布")
                .setParallelism(2);

        env
                .fromElements(1,2,3,4,5,6,7,8,9)
                .setParallelism(1)
                .rebalance()
                .print("rebalance 轮询分布")
                .setParallelism(2);

        env
                .fromElements(1,2,3,4,5,6,7,8,9)
                .setParallelism(1)
                .broadcast()
                .print("broadcast 广播")
                .setParallelism(2);

        env
                .fromElements(1,2,3,4,5,6,7,8,9)
                .setParallelism(1)
                .global()
                .print("global 全局")
                .setParallelism(2);
        env.execute();
    }
}
