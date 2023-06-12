package day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @Author: L.N
 * @Date: 2023/4/17 1:20
 * @Description:实现连续1秒温度上升
 */
public class TempIncreaseAlert {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SensorSource())
                .keyBy(r -> r.sensorId)
                .process(new TempAlert())
                .print();

        env.execute();
    }

    //定时器负责报警
    //当温度出现上升：当前温度>上一次温度 。注册1秒钟之后的定时器
    //当温度出现下降：当前温度<上一次温度.如果存在报警定时器，则删除定时器
    public static class TempAlert extends KeyedProcessFunction<String,SensorReading,String>{
        //每次需要保存上一次的温度
        private ValueState<Double> lastTemp;
        //因为用到连续1s与时间有关，所以需要时间定时器
        private ValueState<Long> ts;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp",
                    Types.DOUBLE));
            ts = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts", Types.LONG));
        }

        @Override
        public void processElement(SensorReading in, Context ctx, Collector<String> out) throws Exception {
            //将上一次温度存储到一个临时变量。
            Double prevTemp = lastTemp.value();
            //将当前温度保存到lastTemp
            lastTemp.update(in.temperature);

            //判断两种情况
            //如果之间的时间戳不为空，则判断两种情况
            if (prevTemp != null){
                //第一种情况:温度上升&定时器不存在
                if (prevTemp < lastTemp.value() && ts.value() == null){
                    //获取1s钟之后的时间戳
                    long oneSecondLater = ctx.timerService().currentProcessingTime() + 1000L;
                    //注册时间戳
                    ctx.timerService().registerProcessingTimeTimer(oneSecondLater);
                    //更新ts的状态变量值为报警定时器的时间戳
                    ts.update(oneSecondLater);
                }
                //第二种情况：温度下降&定时器存在
                if (prevTemp > lastTemp.value() && ts.value() != null){
                    //删除报警的定时器
                    ctx.timerService().deleteProcessingTimeTimer(ts.value());
                    //清除时间状态变量的值
                    ts.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "连续1s温度上升");
            //由于执行ontimer，flink也会将定时器清空
            //所以保存定时器时间戳的状态变量也需要清空
            ts.clear();
        }
    }

    //设置读数的数据源
    public static class SensorSource implements SourceFunction<SensorReading>{
        //声明两个值，一个随机数，一个flag值
        private boolean running = true;
        private Random random = new Random();
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (running){
                for (int i = 1 ; i < 4 ; i++){
                    ctx.collect(new SensorReading("sensor_"+i, random.nextGaussian()));

                }
                Thread.sleep(300L);
            }
        }

        @Override
        public void cancel() {
        }
    }


    //POJO类，读数事件
    public static class SensorReading{
        public String sensorId;
        public Double temperature;

        public SensorReading() {
        }

        public SensorReading(String sensorId, Double temperature) {
            this.sensorId = sensorId;
            this.temperature = temperature;
        }

        @Override
        public String toString() {
            return "SensorReading{" +
                    "sensorId='" + sensorId + '\'' +
                    ", temperature=" + temperature +
                    '}';
        }
    }
}
