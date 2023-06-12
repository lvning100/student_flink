package util;

/**
 * @Author: L.N
 * @Date: 2023/4/23 14:15
 * @Description: //产品+计数+窗口信息
 */
public class ProductViewCountPerWindow {
    public String productId;
    public Long count;
    public Long windowStartTime;
    public Long windowEndTime;

    public ProductViewCountPerWindow() {
    }

    public ProductViewCountPerWindow(String productId, Long count, Long windowStartTime,
                                     Long windowEndTime) {
        this.productId = productId;
        this.count = count;
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
    }

    @Override
    public String toString() {
        return "ProductViewCountPerWindow{" +
                "productId='" + productId + '\'' +
                ", count=" + count +
                ", windowStartTime=" + windowStartTime +
                ", windowEndTime=" + windowEndTime +
                '}';
    }
}
