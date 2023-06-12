package day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import util.UserBehavior;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author: L.N
 * @Date: 2023/5/4 22:54
 * @Description: 使用FLinkSQL完成TOPN
 */
public class C9_SQL_TOPN {
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
        Table table = streamTableEnvironment
                .fromDataStream(
                    stream,
                    $("userId"),
                    $("productId"),
                    $("categoryId"),
                    $("type"),
                    $("ts").rowtime()
                 );

        //将动态表注册成临时视图
        streamTableEnvironment.createTemporaryView("userbehavior", table);

        //对表进行查询
        //使用Apache Calcite将其解析转换
        //count是全窗口的聚合
        //使用innerSQL对数据进行分组计数
        String innerSQL =
                "SELECT productId,COUNT(productId) as cnt ,"
                + "HOP_START(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS ) AS windowStartTime, "
                + "HOP_END(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS ) AS windowEndTime "
                + "FROM userbehavior GROUP BY productId,HOP(ts,INTERVAL '5' MINUTES,INTERVAL '1' "
                        + "HOURS )";
        //使用midSQL对数据开窗排序
        String midSQL =
                "SELECT * ,ROW_NUMBER() OVER(PARTITION BY windowEndTime ORDER BY cnt DESC) AS row_num "
                + " FROM ("+ innerSQL +")";
        //使用outerSQL进行过滤
        String outerSQL =
                "SELECT * FROM ("+ midSQL +") WHERE row_num <= 3";

        Table result = streamTableEnvironment.sqlQuery(outerSQL);

        streamTableEnvironment.toChangelogStream(result).print();

        env.execute();

    }
}
