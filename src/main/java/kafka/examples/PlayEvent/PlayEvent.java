package kafka.examples.PlayEvent;

import java.io.Serializable;

/**
 * Created by PravinKumar on 31/8/17.
 */
public class PlayEvent implements Serializable {

    private long songId;
    private long duration;

    public PlayEvent(long songId, long duration) {
        this.songId = songId;
        this.duration = duration;
    }

    public long getSongId() {
        return songId;
    }

    public void setSongId(long songId) {
        this.songId = songId;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    @Override
    public String toString() {
        return "PlayEvent{" +
                "songId=" + songId +
                ", duration=" + duration +
                '}';
    }
}
