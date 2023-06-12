package day08;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @Author: L.N
 * @Date: 2023/5/1 13:54
 * @Description: 实现两阶段提交。
 * 中间落盘到硬盘中。
 */
public class Custom_5_TwoParseSubmitSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(10*1000L);

        env
                .addSource(new SourceFunction<Long>() {
                    private boolean running = true;
                    private long count = 1L;
                    @Override
                    public void run(SourceContext<Long> ctx) throws Exception {
                        while (running){
                            ctx.collect(count);
                            count++;
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .addSink(new TranscationalFileSink());
        env.execute();
    }

    public static class TranscationalFileSink extends TwoPhaseCommitSinkFunction<Long,String,Void>{
        private BufferedWriter buffer;
        public TranscationalFileSink(){
            super(StringSerializer.INSTANCE, VoidSerializer.INSTANCE);
        }

        //当第一套数据到达时触发调用
        //每次检查点结束时，触发调用
        @Override
        protected String beginTransaction() throws Exception {
            long timeNow = System.currentTimeMillis();
            int taskId = getRuntimeContext().getIndexOfThisSubtask();
            String transaction = timeNow + "-" + taskId;//文件名
            Path tempFilePath = Paths.get("D:\\student_flink\\src\\main\\resources\\temp\\" + transaction);
            Files.createFile(tempFilePath);
            //将缓冲区设置为临时文件的缓冲区
            this.buffer = Files.newBufferedWriter(tempFilePath);
            return transaction;
        }

        //每来一条数据就写入缓冲区
        @Override
        protected void invoke(String transaction, Long in, Context context) throws Exception {
            buffer.write(in + "\n");
        }

        //预提交，将缓冲区中的数据写入临时文件

        @Override
        protected void preCommit(String transaction) throws Exception {
            buffer.flush();
            buffer.close();
        }

        //sink算子收到检查点完成的通知时触发调用
        @Override
        protected void commit(String transaction) {
            Path tempFilePath =
                    Paths.get("D:\\student_flink\\src\\main\\resources\\temp\\" + transaction);
            if (Files.exists(tempFilePath)){
                try{
                    Path completeFilePath =
                            Paths.get("D:\\student_flink\\src\\main\\resources\\complete\\" +transaction);
                    Files.move(tempFilePath,completeFilePath);
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }

        @Override
        protected void abort(String transaction) {
            Path tempFilePath = Paths.get("D:\\student_flink\\src\\main\\resources\\temp\\" + transaction);
            if (Files.exists(tempFilePath)){
                try{
                    Files.delete(tempFilePath);
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }
    }
}
