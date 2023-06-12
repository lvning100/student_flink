package day09;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import util.Event;

import java.util.HashMap;

/**
 * @Author: L.N
 * @Date: 2023/5/4 15:38
 * @Description: 连续三次登录失败。
 * 使用状态机实现。状态机使用状态
 */
public class C5_ThreeFailTimes3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 1000L), 1000L);
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 2000L), 1000L);
                        ctx.collectWithTimestamp(new Event("user-2", "success", 3000L), 3000L);
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 4000L), 4000L);
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 5000L), 5000L);
                    }

                    @Override
                    public void cancel() {
                    }
                });
        stream
                .keyBy(r -> r.key)
                .process( new StateMachine())
                .print();

        env.execute();
    }

    public static class StateMachine extends KeyedProcessFunction<String,Event,String>{
        private static HashMap<Tuple2<String,String>,String> stateMachine;
        static {
            stateMachine = new HashMap<>();

            // (状态，接收到的事件类型) => 跳转到的事件类型
            stateMachine.put(Tuple2.of("INITIAL","fail"), "S1");
            stateMachine.put(Tuple2.of("INITIAL","success"), "SUCCESS");
            stateMachine.put(Tuple2.of("S1","fail"), "S2");
            stateMachine.put(Tuple2.of("S1","success"), "SUCCESS");
            stateMachine.put(Tuple2.of("S2","fail"), "FAIL");
            stateMachine.put(Tuple2.of("S2","success"), "SUCCESS");
        }

        private ValueState<String> currentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            currentState = getRuntimeContext().getState(
                    new ValueStateDescriptor<String>(
                            "current-state",
                            Types.STRING
                    ));
        }

        @Override
        public void processElement(Event in, Context ctx, Collector<String> out) throws Exception {
            if (currentState.value() == null){
                currentState.update("INITIAL");
            }

            //计算将要跳转的状态
            String nextState = stateMachine.get(Tuple2.of(currentState.value(), in.value));
            if (nextState.equals("FAIL")){
                out.collect(in.key + "连续三次失败");
                currentState.update("S2");
            }else if (nextState.equals("SUCCESS")){
                currentState.update("INITIAL");
            } else{
                currentState.update(nextState);
            }
        }
    }
}
