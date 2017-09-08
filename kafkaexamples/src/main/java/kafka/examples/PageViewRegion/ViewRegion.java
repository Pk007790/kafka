package kafka.examples.PageViewRegion;

import java.io.Serializable;

/**
 * Created by PravinKumar on 7/9/17.
 */
public class ViewRegion implements Serializable {

    private String user;
    private String page;
    private String region;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public String toString() {
        return "ViewRegion{" +
                "user='" + user + '\'' +
                ", page='" + page + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
