package util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Author: L.N
 * @Date: 2023/4/13 10:09
 * @Description: 自定义整数数据源
 */
public class IntSource implements SourceFunction<Integer> {
    private boolean running = true;
    private Random random = new Random();
    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while(running){
            ctx.collect(random.nextInt(1000));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
