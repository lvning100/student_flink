package day05;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import util.ClickEvent;
import util.ClickSource;
import util.UserViewCountPerWindow;

/**
 * @Author: L.N
 * @Date: 2023/4/19 23:10
 * @Description:自定义滚动窗口开窗 使用KeyProcessFunction + MapState
 * mapState中维护的是累加器
 */
public class CustomWindowAPI2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process( new MyTumblingFunctionTimeWindow(5000L))
                .print();
        env.execute();
    }

    public static class MyTumblingFunctionTimeWindow extends KeyedProcessFunction<String, ClickEvent, UserViewCountPerWindow>{
        //定义窗口大小
        private Long windowSize;
        public MyTumblingFunctionTimeWindow(Long windowSize) {
            this.windowSize = windowSize;
        }

        //定义mapState
        //Key是窗口的开始时间和结束时间
        //value是一个累加值
        MapState<Tuple2<Long,Long>,Long> mapState;
        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Tuple2<Long, Long>,Long>(
                    "mapState", Types.TUPLE(Types.LONG,Types.LONG), Types.LONG));
        }

        @Override
        public void processElement(ClickEvent in, Context ctx,Collector<UserViewCountPerWindow> out) throws Exception {
            long cuttentTs = ctx.timerService().currentProcessingTime();
            long windowStartSize = cuttentTs - cuttentTs % windowSize;
            long windowEndTime = windowStartSize + windowSize;

            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartSize, windowEndTime);

            if (!mapState.contains(windowInfo)){
                mapState.put(windowInfo, 1L);
            } else{
                long values = mapState.get(windowInfo) + 1L;
                //hashMap是幂等性的，新put的键值对会更新旧put的键值对
                mapState.put(windowInfo, values);
            }

            ctx.timerService().registerProcessingTimeTimer(windowEndTime - 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                            Collector<UserViewCountPerWindow> out) throws Exception {
            long windowEndTime = timestamp + 1;
            long windowStartTime = windowEndTime - windowSize;
            String currentKey = ctx.getCurrentKey();
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);
            Long values = mapState.get(windowInfo);
            out.collect(new UserViewCountPerWindow(currentKey,values,windowStartTime,windowEndTime));
        }
    }
}
