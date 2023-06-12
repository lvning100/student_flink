package day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: L.N
 * @Date: 2023/5/5 11:55
 * @Description:
 */
public class C11_SQL_DDL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().build());


        streamTableEnvironment.executeSql(
                "CREATE TABLE clicks ("
                        + "`user` String,`url` String ,`ts` TimeStamp(3),"
                        + "WATERMARK FOR `ts` AS `ts` - INTERVAL '3' SECONDS )"
                        + "WITH ("
                        + "'connector' = 'filesystem',"
                        + "'path' = 'D:\\student_flink\\src\\main\\resources\\file1.csv',"
                        + "'format' = 'csv')");
        streamTableEnvironment.executeSql(
                "CREATE TABLE ResultTable ("
                        + " `user` STRING ,`cnt` BIGINT , "
                        + "windowStartTime TIMESTAMP(3),"
                        + "windowEndTime TIMESTAMP(3))"
                        + "WITH('connector' = 'print')");

        streamTableEnvironment.executeSql(
                "INSERT INTO ResultTable "
                        + "SELECT `user`,COUNT(`user`) AS cnt,"
                        + "TUMBLE_START(`ts`,INTERVAL '5' SECONDS) AS windowStartTime,"
                        + "TUMBLE_END(`ts`,INTERVAL '5' SECONDS) AS windowENDTime "
                        + "FROM clicks GROUP BY `user`,TUMBLE(`ts`,INTERVAL '5' SECONDS)");

    }
}
