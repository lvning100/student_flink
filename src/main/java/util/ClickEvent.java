package util;

import java.sql.Timestamp;

/**
 * @Author: L.N
 * @Date: 2023/4/11 23:05
 * @Description: 点击事件
 */
//使用sacla case class ClickEvent{username:String ，url:String，ts:Long}
public class ClickEvent {
    public String username;
    public String url;
    public Long ts;

    public ClickEvent() {
    }

    public ClickEvent(String username, String url, Long ts) {
        this.username = username;
        this.url = url;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "ClickEvent{"+
                "username=‘" + username +'\''+
                ",url=‘" + url +'\''+
                ",ts=‘" + new Timestamp(ts) +'\''+
                "}";
    }
}
