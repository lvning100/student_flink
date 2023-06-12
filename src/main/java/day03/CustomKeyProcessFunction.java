package day03;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: L.N
 * @Date: 2023/4/14 11:07
 * @Description: 使用KeyProcessFunction实现CustomProcessFunction的功能
 * 实现key是使用keySelector
 */
public class CustomKeyProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("white","black","blue")
                //将所有的数据都发到Stringd的key下
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String in) throws Exception {
                        return "String";
                    }
                })
                //使用keyProcessFunction处理经过key的数据,
                //这里只用到了out向下输出，没有使用ctx上下文
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        if (in.equals("white")){
                            out.collect(in);
                        }else if (in.equals("black")){
                            out.collect(in);
                            out.collect(in);
                        }else {
                            out.collect("其他");
                        }
                    }
                })
                .print();
        env.execute();
    }
}
