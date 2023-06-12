package day07;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * @Author: L.N
 * @Date: 2023/4/28 16:27
 * @Description: select * from A inner join B on A.key = B.key
 * 将来自两条流的相同key的数据两两配对。
 * 因为两两配对，所以需要把相同key的数据都保存下来。
 * 我们要完成来一条数据，需要和另一条数据的所有的key进行join。所以需要两个列表状态变量
 * 将列表中的状态变量的泛型设置为String。
 * 这里的状态变量是键控状态变量，每一个key都会维护自己的状态变量。
 * 只要调用processElement不管是访问定时器还是状态变量都会把读写的范围限制在输入数据的key所对应的状态变量
 */
public class Custom_8_SearchStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, String>> stream1 = env.fromElements(
                Tuple2.of("a", "left1"),
                Tuple2.of("b", "left1"),
                Tuple2.of("a", "left2"),
                Tuple2.of("b", "left2")
                );

        DataStreamSource<Tuple2<String, String>> stream2 = env.fromElements(
                Tuple2.of("a", "right1"),
                Tuple2.of("a", "right2"),
                Tuple2.of("b", "right1"),
                Tuple2.of("b", "right2")
        );

        stream1
                .keyBy(r -> r.f0)
                .connect(stream2.keyBy(r ->r.f0))
                .process(new InnerJoin())
                .print();
        env.execute();
    }

    public static class InnerJoin extends CoProcessFunction<Tuple2<String,String>,Tuple2<String, String>,String>{
        private ListState<Tuple2<String,String>> listState1;
        private ListState<Tuple2<String,String>> listState2;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState1 = getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple2<String, String>>("listState1",
                            Types.TUPLE(Types.STRING,Types.STRING))
            );
            listState2 = getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple2<String, String>>("listState2",
                            Types.TUPLE(Types.STRING,Types.STRING))
            );
        }

        @Override
        public void processElement1(Tuple2<String, String> in1, Context ctx,
                                    Collector<String> out) throws Exception {
            //对history1的访问范围局限到了in1 的key对应的列表
            listState1.add(in1);

            //对history2的访问范围局限到了in1的key所对应的列表
            for (Tuple2<String, String> e : listState2.get()) {
                out.collect(in1 +"=>"+ e);
            }
        }

        @Override
        public void processElement2(Tuple2<String, String> in2, Context ctx,
                                    Collector<String> out) throws Exception {
            listState2.add(in2);
            for (Tuple2<String, String> e : listState1.get()) {
                //从左到右打印，从第一条流打印到第二条流
                out.collect(e + "=>" + in2);
            }
        }
    }
}
