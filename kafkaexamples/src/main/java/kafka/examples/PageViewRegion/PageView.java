package kafka.examples.PageViewRegion;

import java.io.Serializable;

/**
 * Created by PravinKumar on 7/9/17.
 */
public class PageView implements Serializable {

    private String name;
    private String page;
    private String region;

    public static PageView getInstance(){

        return new PageView();
    }

    public PageView() {
    }

    public PageView(String name, String page, String region) {
        this.name = name;
        this.page = page;
        this.region = region;
    }

    public PageView(String name, String page) {
        this.name = name;
        this.page = page;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
        return "PageView{" +
                "name='" + name + '\'' +
                ", page='" + page + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
