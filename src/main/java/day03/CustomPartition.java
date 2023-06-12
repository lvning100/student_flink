package day03;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: L.N
 * @Date: 2023/4/13 15:02
 * @Description: 自定义分区
 * 之前使用key的时候，数据去向哪一个并行子任务我们无法决定。但是使用自定义分区可以决定数据去向哪一个并行子任务
 */
public class CustomPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                //匿名类：用来设定将哪一个key的数据发送到哪一个并行子任务中
                //两个参数，第一个参数 根据key分配到不同的并行子任务。指明并行子任务的索引就可以。
                //第二个参数：将值分配到多个key
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        //如果输入数据的key是0，将数据路由到print的索引为0的并行子任务
//                        方法一：
//                      return key;
                        if (key == 0) {
                            return 0 ;
                        } else if(key == 1 ) {
                            return  1;
                        } else
                            return 2;
                    }
                },
                        //指定数据的数据key
                        new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer in) throws Exception {
                        return in % 3;
                    }
                })
                .print()
                .setParallelism(4);
        env.execute();
    }
}
