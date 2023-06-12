package day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import util.ProductViewCountPerWindow;
import util.UserBehavior;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @Author: L.N
 * @Date: 2023/4/26 18:03
 * @Description: 实时热门商品
 * 计滑动窗口(窗口长度1小时，滑动距离5分钟)里面浏览次数最多的3个商品
 */
public class Custom_9_TOP3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\student_flink\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String in) throws Exception {
                        String[] array = in.split(",");
                        return new UserBehavior(
                                //用户Id ，商品Id ，品类Id ,类型 ，事件时间戳
                                array[0],
                                array[1],
                                array[2],
                                array[3],
                                Long.parseLong(array[4]) * 1000L
                        );
                    }
                })
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior in) throws Exception {
                        return in.type.equals("pv");
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .keyBy(r -> r.productId)
                //使用KeyProcessFunction实现滑动窗口
                .process(new MySlidingEventTimeWindow(60 * 60 * 1000L, 5 * 60 * 1000L))
                .keyBy(new KeySelector<ProductViewCountPerWindow, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(ProductViewCountPerWindow in) throws Exception {
                        return Tuple2.of(in.windowStartTime,in.windowEndTime);
                    }
                })
                .process(new TopN(3))
                .print();
        env.execute();
    }

    public static class MySlidingEventTimeWindow extends KeyedProcessFunction<String,UserBehavior
            , ProductViewCountPerWindow>{
        private long windowSize;
        private long windowSlide;

        public MySlidingEventTimeWindow(long windowSize, long windowSlide) {
            this.windowSize = windowSize;
            this.windowSlide = windowSlide;
        }

        //设置状态变量 mapState Key：windowinfo，Value：acc
        private MapState<Tuple2<Long,Long>,Long> mapstate;
        //初始化

        @Override
        public void open(Configuration parameters) throws Exception {
            mapstate = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Tuple2<Long, Long>, Long>("mapstate",
                            Types.TUPLE(Types.LONG,Types.LONG), Types.LONG)
            );
        }

        //每来一条数据处理一次
        @Override
        public void processElement(UserBehavior in, Context ctx,
                                   Collector<ProductViewCountPerWindow> out) throws Exception {
            //先计算所属的第一个滑动窗口的开始时间
            long startTime = in.ts - in.ts % windowSlide; //第一个窗口的开始时间
            ArrayList<Tuple2<Long,Long>> windowInfoArray = new ArrayList<>();
            for (long start= startTime ;start > in.ts - windowSize;start -= windowSlide){
                windowInfoArray.add(Tuple2.of(start,start + windowSize));
            }

            for (Tuple2<Long,Long> windowInfo : windowInfoArray){
                if (!mapstate.contains(windowInfo)){
                    mapstate.put(windowInfo,1L);
                } else{
                    mapstate.put(windowInfo, mapstate.get(windowInfo)+1L);
                }
                ctx.timerService().registerEventTimeTimer(windowInfo.f1 - 1L);
            }
        }

        //定时器，一个周期执行一次
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<ProductViewCountPerWindow> out) throws Exception {
            long windowEndTime = timestamp + 1L;
            long windowStartTime = windowEndTime - windowSize;
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);
            Long count = mapstate.get(windowInfo);
            String productId = ctx.getCurrentKey();
            out.collect(new ProductViewCountPerWindow(productId, count, windowStartTime, windowEndTime));
            //执行之后删除窗口的信息和其对应的值
            mapstate.remove(windowInfo);
        }
    }

    public static class TopN extends KeyedProcessFunction<Tuple2<Long,Long>,
            ProductViewCountPerWindow,
            String>{
        private int n;

        public TopN(int n) {
            this.n = n;
        }
        //声明一个列表状态变量，用来存储上一个KeyBy之后的数据
        private ListState<ProductViewCountPerWindow> listState;

        //初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ProductViewCountPerWindow>(
                            "liststate"
                    , Types.POJO(ProductViewCountPerWindow.class))
            );
        }

        @Override
        public void processElement(ProductViewCountPerWindow in, Context ctx,
                                   Collector<String> out) throws Exception {
            listState.add(in);
            //保证所有的属于in.windowStartTime ~ in.windowEndTime的ProductViewCountPerWindow都添加到listState中
            ctx.timerService().registerEventTimeTimer(in.windowEndTime + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ProductViewCountPerWindow> arrayList = new ArrayList<>();
            for (ProductViewCountPerWindow p : listState.get()) {
                arrayList.add(p);
            }
            //手动GC
            listState.clear();

            arrayList.sort(new Comparator<ProductViewCountPerWindow>() {
                @Override
                public int compare(ProductViewCountPerWindow o1, ProductViewCountPerWindow o2) {
                    return (int)(o2.count-o1.count);
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("========="+ctx.getCurrentKey().f0+"==============="+ctx.getCurrentKey().f1+"===============================\n");
            for(int i = 0 ;i < n ;i++ ){
                ProductViewCountPerWindow tmp =arrayList.get(i);
                result.append("第" + (i+1) + "名的商品ID：" + tmp.productId + ",浏览次数：" + tmp.count +
                        "\n");
            }
            result.append("=================================================================================\n");
            out.collect(result.toString());


        }
    }
}
