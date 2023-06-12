package day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.IntSource;
import util.Statistic;

/**
 * @Author: L.N
 * @Date: 2023/4/13 10:14
 * @Description: 对整型数据源的测试，计算所有历史数据的最大值，最小值，平均值，总和，以及条数
 */
public class IntSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
//                .fromElements(1,10)
                .addSource(new IntSource())
                .map(new MapFunction<Integer, Statistic>() {
                    @Override
                    public Statistic map(Integer in) throws Exception {
                        return new Statistic(
                                in,
                                in,
                                in,
                                in,
                                1
                        );
                    }
                })
                .keyBy(r -> "int")
                .reduce(new ReduceFunction<Statistic>() {
                    @Override
                    public Statistic reduce(Statistic acc, Statistic in) throws Exception {
                        return new Statistic(
                                Math.max(in.max, acc.max),
                                Math.min(in.min, acc.min),
                                (in.sum + acc.sum) / (1 + acc.count),
                                in.sum + acc.sum,
                                1 + acc.count

                        );
                    }
                })
                .print();

        env.execute();
    }
}
