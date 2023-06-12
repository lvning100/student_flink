package util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @Author: L.N
 * @Date: 2023/4/11 23:17
 * @Description: 自定义数据源 单数据源
 */
public class ClickSource implements SourceFunction<ClickEvent> {
    //定义一个标志位，用来无限的发送数据配合while
    // 结束的时候更改标志位的值
    private boolean running = true;
    //定义一个随机数
    private Random random = new Random();
    private String[] userArray = {"Mary","Bob","Alice"};
    private String[] urlArray = {"./home","./cart","./buy"};

    @Override
    public void run(SourceContext<ClickEvent> ctx) throws Exception {
        while (running){
            ctx.collect(new ClickEvent(
                    userArray[random.nextInt(userArray.length)],
                    urlArray[random.nextInt(urlArray.length)],
                    //获得当前系统的毫秒时间戳
                    Calendar.getInstance().getTimeInMillis()
            ));
            //减缓数据的产生，每次产生一个之后休眠1秒钟
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {

    }
}