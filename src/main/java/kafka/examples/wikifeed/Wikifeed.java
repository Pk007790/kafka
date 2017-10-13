package kafka.examples.wikifeed;

import java.io.Serializable;

/**
 * Created by PravinKumar on 29/7/17.
 */
public class Wikifeed implements Serializable {

    private String name;
    private boolean isNew;
    private String content;

    public Wikifeed(String name, boolean isNew, String content) {
        this.name = name;
        this.isNew = isNew;
        this.content = content;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isNew() {
        return isNew;
    }

    public void setNew(boolean aNew) {
        isNew = aNew;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
