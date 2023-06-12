package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: L.N
 * @Date: 2023/4/8 22:56
 * @Description: copy
 */
public class WordCount1 {
    public static void main(String [] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .socketTextStream("hadoop",9999)
                .setParallelism(1)
                .flatMap(new Tokenizer())
                .setParallelism(1)
                .keyBy(value -> value.f0)
                .reduce(new WordCount2())
                .setParallelism(1)
                .print()
                .setParallelism(1);

        env.execute();


    }

    public static class WordCount2 implements ReduceFunction<Tuple2<String,
            Integer>>{
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> acc,
                                              Tuple2<String, Integer> value2) throws Exception {
            return Tuple2.of(acc.f0,acc.f1+value2.f1);
        }
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }
    }
}
