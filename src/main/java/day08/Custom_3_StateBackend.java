package day08;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.ClickSource;

/**
 * @Author: L.N
 * @Date: 2023/4/29 15:57
 * @Description: 设置检查点
 */
public class Custom_3_StateBackend {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .enableCheckpointing(10 * 1000L)
                .setStateBackend(new FsStateBackend("file:\\" + "D:\\student_flink\\src\\main"
                        + "\\resources\\ckpts"));

        env
                .addSource(new ClickSource())
                .print("设置检查点");
        env.execute();
    }
}
