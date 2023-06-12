package util;

/**
 * @Author: L.N
 * @Date: 2023/4/13 10:16
 * @Description: 定义一个POJO类，用来保存统计数据
 * POJO类的特点 ，类是公共的，有一个空参构造器，字段都是公共的或者私有但是提供getset方法
 */
public class Statistic {

    public Integer max;
    public Integer min;
    public Integer avg;
    public Integer sum;
    public Integer count;


    public Statistic() {
    }

    public Statistic(Integer max, Integer min, Integer avg, Integer sum, Integer count) {
        this.max = max;
        this.min = min;
        this.avg = avg;
        this.sum = sum;
        this.count = count;
    }


    @Override
    public String toString() {
        return "Statistic{" +
                "max=" + max +
                ", min=" + min +
                ", avg=" + avg +
                ", sum=" + sum +
                ", count=" + count +
                '}';
    }
}
