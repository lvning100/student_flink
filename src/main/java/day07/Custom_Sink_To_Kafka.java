package day07;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Author: L.N
 * @Date: 2023/4/26 20:38
 * @Description: 输出文件到Kafka
 */
public class Custom_Sink_To_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");

        env
                .readTextFile("D:\\student_flink\\src\\main\\resources\\UserBehavior.csv")
                .addSink(new FlinkKafkaProducer<String>(
                        "topic-userbehavior",
                        new SimpleStringSchema(),
                        properties
                ));
        env.execute();
    }
}
