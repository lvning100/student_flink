package day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: L.N
 * @Date: 2023/4/14 10:42
 * @Description: 自定义实现processFunction
 * 简单的完成Flatmap的功能,这个程序没有使用ctx上下文功能
 */
public class CustomProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("white","black","blue")//数据源是一个集合
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        if (in.equals("white") ){
                            out.collect(in);
                        } else if(in.equals("black")){
                            out.collect(in);
                            out.collect(in);
                        } else
                            out.collect("其他");
                    }
                })
                .setParallelism(1)
                .print("使用ProcessFunction完成flatmap")
                .setParallelism(1);
        env.execute();
    }

}
