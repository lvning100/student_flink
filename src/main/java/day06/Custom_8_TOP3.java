package day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.ProductViewCountPerWindow;
import util.UserBehavior;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @Author: L.N
 * @Date: 2023/4/25 20:14
 * @Description: 实时热门商品
 */
public class Custom_8_TOP3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\student_flink\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    //用户id ，商品id ，品类id，页面类型，事件时间
                    @Override
                    public UserBehavior map(String in) throws Exception {
                        String[] array = in.split(",");
                        return new UserBehavior(
                                array[0],
                                array[1],
                                array[2],
                                array[3],
                                Long.parseLong(array[4])*1000L
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
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(2)))
                .aggregate(new AggCount(), new ResultWindow())

                .keyBy(new KeySelector<ProductViewCountPerWindow, Tuple2<Long,Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(ProductViewCountPerWindow in) throws Exception {
                        return Tuple2.of(in.windowStartTime, in.windowEndTime);
                    }
                })
                .process(new TopN(3))
                .print();

        env.execute();
    }
    public static class TopN extends KeyedProcessFunction<Tuple2<Long,Long>, ProductViewCountPerWindow,String> {
        private int n;
        public TopN(int n) {
            this.n = n;
        }

        //使用排序的话需要把数据放入ArrayList中，之后写Compare比较器
        private ListState<ProductViewCountPerWindow>  liststate;
        @Override
        public void open(Configuration parameters) throws Exception {
            liststate =
                    getRuntimeContext().getListState(new ListStateDescriptor<ProductViewCountPerWindow>(
                            "liststate",
                            Types.POJO(ProductViewCountPerWindow.class)
                    ));
        }
        @Override
        public void processElement(ProductViewCountPerWindow in, Context ctx, Collector<String> out) throws Exception {
            //每来一条数据就放入列表状态变量
            liststate.add(in);
            //并注册定时器：当前数据所在窗口的结束时间+1ms
            ctx.timerService().registerEventTimeTimer(in.windowEndTime + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ProductViewCountPerWindow> arrayList = new ArrayList<>();
            //将所有的数据放入arraylist列表中，并在列表中对数据进行排序
            for (ProductViewCountPerWindow p : liststate.get()) {
                arrayList.add(p);
            }
            //手动GC清除列表状态变量 ,这清除的是当前key在定时器的列表
            liststate.clear();

            //排序
            arrayList.sort(new Comparator<ProductViewCountPerWindow>() {
                @Override
                public int compare(ProductViewCountPerWindow o1, ProductViewCountPerWindow o2) {
                    return (int) (o2.count-o1.count);
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("=========="+ new Timestamp(ctx.getCurrentKey().f0) +"~" +new Timestamp(ctx.getCurrentKey().f1) + "============\n");
            for (int i = 0; i < n; i++) {
                ProductViewCountPerWindow tmp = arrayList.get(i);
                result.append("第"+(i+1)+"名的商品ID："+tmp.productId +"浏览次数是:"+tmp.count+"\n");
            }
            result.append("==========================================");
            out.collect(result.toString());

        }
    }

    public static class AggCount implements AggregateFunction<UserBehavior,Long, Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class ResultWindow extends ProcessWindowFunction<Long, ProductViewCountPerWindow, String,TimeWindow>{

        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
            out.collect(
                    new ProductViewCountPerWindow(key, elements.iterator().next(),
                            context.window().getStart(),context.window().getEnd())
            );
        }
    }
}
