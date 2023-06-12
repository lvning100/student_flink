package util;

/**
 * @Author: L.N
 * @Date: 2023/4/17 15:18
 * @Description: 用来输出每个窗口中每个用户的统计浏览页面次数
 */
public class UserViewCountPerWindow {
    public String username;
    public Long count;
    public long windowsStart;
    public long windowsEnd;

    public UserViewCountPerWindow() {    }
    public UserViewCountPerWindow(String username,long count,long windowsStart,long windowsEnd){
        this.username =username;
        this.count= count;
        this.windowsStart = windowsStart;
        this.windowsEnd = windowsEnd;
    }

    @Override
    public String toString() {
        return "UserCountUrlPerWindow{" +
                "username='" + username + '\'' +
                ", count='" + count + '\'' +
                ", windowsStart=" + windowsStart +
                ", windowsEnd=" + windowsEnd +
                '}';
    }
}
