package util;

/**
 * @Author: L.N
 * @Date: 2023/4/28 17:58
 * @Description:
 */
public class Event {
    public String key;
    public String value;
    public Long ts;

    public Event() {
    }

    public Event(String key, String value, Long ts) {
        this.key = key;
        this.value = value;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Event{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", ts=" + ts +
                '}';
    }
}
