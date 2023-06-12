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

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: L.N
 * @Date: 2023/4/19 22:10
 * @Description: 自定义滚动窗口开窗 使用KeyProcessFunction + MapState
 * MapState中维护列表
 */
public class CustomWindowAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process(new MyTumblingFunctionTimeWindow(5000L))
                .print();
        env.execute();
    }

    public static class MyTumblingFunctionTimeWindow extends KeyedProcessFunction<String,
            ClickEvent, UserViewCountPerWindow>{
        //定义窗口大小，这就是滚动窗口的大小
        private Long windowSize;
        public MyTumblingFunctionTimeWindow(Long windowSize) {
            this.windowSize = windowSize;
        }
        //key:(窗口的开始时间，窗口的结束时间)
        // value：(窗口的元素列表)
        private MapState<Tuple2<Long,Long>, List<ClickEvent>> mapState;
        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Tuple2<Long, Long>,
                    List<ClickEvent>>(
                    "mapState",
                    Types.TUPLE(Types.LONG, Types.LONG),
                    Types.LIST(Types.POJO(ClickEvent.class)))
            );
        }

        @Override
        public void processElement(ClickEvent in, Context ctx,
                                   Collector<UserViewCountPerWindow> out) throws Exception {

            //获得当前的进程时间
            long currentTs = ctx.timerService().currentProcessingTime();
            //计算到来的数据时间属于那个窗口
            long windowStartTime = currentTs - currentTs % windowSize;
            long windowEndTime = windowStartTime + windowSize;

            //生成一个窗口信息的二元组，一会做mapState的Key
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);

            //判定：是否是第一次来的数据。原因：如果来的数据没有窗口，需要现new一个窗口
            //如果当前窗口信息不存在则new一个，否则直接将输入的数据添加到该窗口中
            if (!mapState.contains(windowInfo)){
                //新建一个ArrayList数组
                ArrayList<ClickEvent> events = new ArrayList<>();
                //添加数据
                events.add(in);
                //将窗口信息和列表信息放入MapState中
                mapState.put(windowInfo, events);
            } else {
                //mapState.get(windowInfo) 获取窗口Key的value
                mapState.get(windowInfo).add(in);
            }

            //注册一个定时器 定时器是窗口结束时间-1ms
            ctx.timerService().registerProcessingTimeTimer(windowEndTime - 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                            Collector<UserViewCountPerWindow> out) throws Exception {
            //根据timestamp计算出窗口的开始时间和结束时间
            long windowEndTime = timestamp + 1L;
            long windowStartTime = windowEndTime - windowSize;
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);
            //通过窗口信息获取value
            //使用ArrayList的size方法得到计数值
            int count = mapState.get(windowInfo).size();
            String currentKey = ctx.getCurrentKey();
            out.collect(new UserViewCountPerWindow(currentKey, count, windowStartTime, windowEndTime));

            //销毁窗口信息
            mapState.remove(windowInfo);
        }
    }
}
