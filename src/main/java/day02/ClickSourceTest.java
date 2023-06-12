package day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.ClickSource;

/**
 * @Author: L.N
 * @Date: 2023/4/11 23:30
 * @Description: 自定义数据源测试
 */
public class ClickSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new ClickSource()).print();

        env.execute();
    }
}
