package day04;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import util.ClickEvent;
import util.ClickSource;

/**
 * @Author: L.N
 * @Date: 2023/4/17 0:11
 * @Description: 自定义实现MapState
 * 实现每来一条数据，显示这条数据的用户名所有的点击信息
 */
public class CustomMapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process(new UserUrlCount())
                .print();
        env.execute();

    }

    public static class UserUrlCount extends KeyedProcessFunction<String, ClickEvent,String>{
        //声明MapState状态变量
        // key是输入数据key的key ： url
        //value是输入数据key的key的value ： count
        //输入数据的key通过ctx.getCurrentKey() 获取
        private MapState<String ,Integer> urlCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            urlCount = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, Integer>("urlCount", Types.STRING, Types.INT)
            );
        }

        @Override
        public void processElement(ClickEvent in, Context ctx, Collector<String> out) throws Exception {
            //赋予状态变量初始值
            //in.url 就是当前urlcount状态变量的key
            if (urlCount.get(in.url) == null){
                urlCount.put(in.url, 1);
            } else{
                urlCount.put(in.url, urlCount.get(in.url) + 1);
            }

            String result = "";
            result = result + ctx.getCurrentKey() + "{\n";
            //通过mapState.keys() 方法遍历当前mapState的所有key
            for (String key : urlCount.keys()) {
                result = result + key +"\t" + urlCount.get(key)+ "{\n";
            }
            out.collect(result + "}");
        }
    }
}
