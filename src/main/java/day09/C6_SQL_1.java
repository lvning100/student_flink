package day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import util.UserBehavior;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author: L.N
 * @Date: 2023/5/4 17:12
 * @Description: 过滤出PV的输出，转换成表格的形式，在转换成流
 * 使用UserBehavior数据集
 */
public class C6_SQL_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> stream = env
                .readTextFile("D:\\student_flink\\src\\main\\resources\\UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String in, Collector<UserBehavior> out) throws Exception {
                        String[] array = in.split(",");
                        UserBehavior userBehavior = new UserBehavior(
                                array[0],
                                array[1],
                                array[2],
                                array[3],
                                Long.parseLong(array[4]) * 1000L
                        );

                        if (userBehavior.type.equals("pv")) {
                            out.collect(userBehavior);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element,
                                                         long recordTimestamp) {
                                return element.ts;
                            }
                        })
                );

        //获取表的执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().build());

        //将数据流转换成动态表
        Table table = streamTableEnvironment.fromDataStream(
                stream,
                $("userId"),
                $("productId"),
                $("categoryId"),
                $("type"),
                $("ts").rowtime() //rowtime() 表示这一列是事件时间
        );
        //将动态表转换成数据流
        DataStream<Row> result = streamTableEnvironment.toChangelogStream(table);

        //输出数据流
        result.print();


        env.execute();
    }
}
