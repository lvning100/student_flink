package day09;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import util.Event;

/**
 * @Author: L.N
 * @Date: 2023/5/4 9:11
 * @Description: 订单超时检测，使用底层API实现
 */
public class C4_OrderTimeOut2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<Event, String> stream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("order-1", "create-order", 1000L),
                                1000L);
                        ctx.collectWithTimestamp(new Event("order-2", "create-order", 2000L),
                                2000L);
                        ctx.collectWithTimestamp(new Event("order-1", "pay-order", 3000L), 3000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> r.key);
        //对分租之后的数据流进行操作
        stream.process(new KeyedProcessFunction<String, Event, String>() {
            //声明值状态变量
            private ValueState<Event> state;
            //对状态变量进行初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(
                        new ValueStateDescriptor<Event>(
                                "state",
                                Types.POJO(Event.class)
                        )
                );
            }

            //当来一条创建订单数据
            @Override
            public void processElement(Event in, Context ctx, Collector<String> out) throws Exception {
                if (in.value.equals("create-order")){
                    state.update(in);
                    ctx.timerService().registerEventTimeTimer(in.ts + 5000L);
                } else if (in.value.equals("pay-order")){
                    out.collect(in.key+"在" + in.ts+ "完成支付");
                    state.clear();
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                if (state.value() != null){
                    out.collect(state.value().key+"超时未支付");
                }
            }
        })
                .print();
        env.execute();
    }
}
