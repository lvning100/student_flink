package day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: L.N
 * @Date: 2023/5/5 10:39
 * @Description: 使用DDL的方式编写FlinkSQL
 */
public class C10_SQL_DDL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().build());

        streamTableEnvironment.executeSql(
                "CREATE TABLE clicks ("
                        + " `user` STRING , `url` STRING) "
                        + "WITH ("
                        + "'connector' = 'filesystem',"
                        + "'path' = 'D:\\student_flink\\src\\main\\resources\\file.txt',"
                        + "'format' = 'csv' )");
        streamTableEnvironment.executeSql(
                "CREATE TABLE ResultTable ("
                        + "`use` STRING ,`cnt`  BIGINT"
                        + ") WITH("
                        + "'connector' = 'print' )");
        streamTableEnvironment.executeSql(
                "INSERT INTO ResultTable "
                        + " SELECT user , COUNT(user) as cnt "
                        + "FROM clicks "
                        + "GROUP BY user");
    }
}
