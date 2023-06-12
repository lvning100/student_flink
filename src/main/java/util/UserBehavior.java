package util;

/**
 * @Author: L.N
 * @Date: 2023/4/23 14:11
 * @Description: 关于解析数据源的POJO类 解析的数据源是UserBehavior.csv
 * 用户id，产品id，品类id，类型，时间戳
 */
public class UserBehavior {
    public String userId;
    public String productId;
    public String categoryId;
    public String type;
    public Long ts;


    public UserBehavior() {
    }

    public UserBehavior(String userId, String productId, String categoryId, String type,
                        Long ts) {
        this.userId = userId;
        this.productId = productId;
        this.categoryId = categoryId;
        this.type = type;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", categotyId='" + categoryId + '\'' +
                ", type='" + type + '\'' +
                ", ts='" + ts + '\'' +
                '}';
    }
}

