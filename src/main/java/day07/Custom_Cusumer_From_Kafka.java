package day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import util.ProductViewCountPerWindow;
import util.UserBehavior;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

/**
 * @Author: L.N
 * @Date: 2023/4/26 22:39
 * @Description: 从Kafka中读取数据
 * 统计滑动窗口(窗口长度1小时，滑动距离5分钟)里面浏览次数最多的3个商品
 */
public class Custom_Cusumer_From_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");

        env
                .addSource(new FlinkKafkaConsumer<String>(
                        "topic-userbehavior",
                        new SimpleStringSchema(),
                        properties))
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String in, Collector<UserBehavior> out) throws Exception {
                        String[] array = in.split(",");
                        //userid ,productid,categoryid ,type,ts
                        UserBehavior userBehavior = new UserBehavior(
                                array[0], array[1], array[2], array[3],
                                Long.parseLong(array[4]) * 1000L
                        );

                        if (userBehavior.type.equals("pv")) {
                            out.collect(userBehavior);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element,
                                                                 long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                )
                .keyBy(r -> r.productId)
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.minutes(5)))
                .aggregate(new AggCount(), new ResultWindow())
                .keyBy(new KeySelector<ProductViewCountPerWindow, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(ProductViewCountPerWindow in) throws Exception {
                        return Tuple2.of(in.windowStartTime, in.windowEndTime);
                    }
                })
                .process(new TopN(3))
                .print();
        env.execute();
    }

    public static class TopN extends KeyedProcessFunction<Tuple2<Long, Long>, ProductViewCountPerWindow, String> {
        //接受指定的输出前几名
        private int n;

        public TopN(int n) {
            this.n = n;
        }

        //声明状态变量
        private ListState<ProductViewCountPerWindow> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState =
                    getRuntimeContext().getListState(new ListStateDescriptor<ProductViewCountPerWindow>("liststate", Types.POJO(ProductViewCountPerWindow.class)));
        }
        //每来一条数据处理一条数据

        @Override
        public void processElement(ProductViewCountPerWindow in, Context ctx,
                                   Collector<String> out) throws Exception {
            //将数据加入列表的状态变量
            listState.add(in);
            //幂等性 ： 注册数据所在窗口的结束时间+1L 为定时器
            //保证所有的属于in.windowStartTime ~ in.windowEndTime的ProductViewCountPerWindow都添加到listState中
            ctx.timerService().registerEventTimeTimer(in.windowEndTime + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //声明一个arraylist，将状态中的信息全部放入，用来排序
            ArrayList<ProductViewCountPerWindow> arrayList = new ArrayList<>();
            for (ProductViewCountPerWindow p : listState.get()) {
                arrayList.add(p);
            }
            arrayList.sort(new Comparator<ProductViewCountPerWindow>() {
                @Override
                public int compare(ProductViewCountPerWindow o1, ProductViewCountPerWindow o2) {
                    return (int) (o2.count - o1.count);
                }
            });

            //准备输出数据
            StringBuilder result = new StringBuilder();
            result.append("=========" + ctx.getCurrentKey().f0 + "==============" + ctx.getCurrentKey().f1 + "============\n");
            for (int i = 0; i < n; i++) {
                ProductViewCountPerWindow p = arrayList.get(i);
                result.append("第" + (i + 1) + "个商品的ID是" + p.productId + "商品的销售个数是:" + p.count + "\n");
            }
            result.append("=============================================================\n");
            out.collect(result.toString());
        }
    }

        public static class ResultWindow extends ProcessWindowFunction<Long, ProductViewCountPerWindow
                , String, TimeWindow> {
            @Override
            public void process(String key, Context ctx, Iterable<Long> elements,
                                Collector<ProductViewCountPerWindow> out) throws Exception {
                out.collect(new ProductViewCountPerWindow(
                        key,
                        elements.iterator().next(),
                        ctx.window().getStart(),
                        ctx.window().getEnd()
                ));
            }
        }

        public static class AggCount implements AggregateFunction<UserBehavior, Long, Long> {
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
}
