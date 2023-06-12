package day02;

import org.apache.flink.util.MathUtils;

/**
 * @Author: L.N
 * @Date: 2023/4/12 23:41
 * @Description: Key的底层源码
 */
public interface KeyHashCode {
    public static void main(String[] args) {
        // 设置key
        Integer key = 1;
        //获取key的hashcode值
        int hashCode = key.hashCode();
        //计算key的hashcode的murmurHash
        int murmurHash = MathUtils.murmurHash(hashCode);
        //设置默认的最大并行度是128
        //reduce的并行度是4
        int idx = (murmurHash %128) * 4 /128;
        System.out.println(idx);
    }
}
