package day07;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import util.ClickEvent;
import util.ClickSource;

import javax.management.Query;

/**
 * @Author: L.N
 * @Date: 2023/4/28 13:14
 * @Description: 查询流
 * 多流合并
 * 这个例子实现的功能是：
 *  在 nc -lk 9999 中输入mary，就在控制台中输出Mary的访问数据
 *  再次输入Alice则停止输出Mary的数据，转而输出Alice的数据
 *
 */
public class Custom_7_SearchStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<ClickEvent> clickEventDataStreamSource = env.addSource(new ClickSource());
        DataStreamSource<String> queryStream = env.socketTextStream("hadoop102", 9999);

        clickEventDataStreamSource
                .connect(queryStream)
                .flatMap(new Query())
                .print();
        env.execute();
    }
    public static class Query implements CoFlatMapFunction<ClickEvent,String,ClickEvent>{
        private String queryString = "";
        @Override
        public void flatMap1(ClickEvent value, Collector<ClickEvent> out) throws Exception {
            if (value.username.equals(queryString)){
                out.collect(value);
            };
        }

        @Override
        public void flatMap2(String value, Collector<ClickEvent> out) throws Exception {
            queryString = value;
        }
    }
}
