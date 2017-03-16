package benchtpce.entities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by carlosmorais on 19/11/2016.
 */
public class Entry implements Serializable, Comparable<Entry> {
    private int id; //transaction ID

    private String startTimestamp;
    private long startTimestampMS; //start timestamp in Milliseconds

    private String commitTimestamp;
    private long commitTimestampMS; //commit timestamp in Milliseconds

    private String type; // the type of transaction

    private boolean rollback;

    private List<Write> writeset;

    public Entry() {
        this.writeset = new ArrayList<Write>();
        this.rollback = false;
    }

    public String getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(String startTimestamp) {
        this.startTimestamp = startTimestamp;
        this.startTimestampMS = this.timestampToMs(this.startTimestamp);
    }


    @Override
    public int compareTo(Entry other) {
        return Long.compare(this.getStartTimestamMS(), other.getStartTimestamMS());
    }


    private long timestampToMs(String timestamp){
        timestamp = timestamp.replaceAll(" ", "");
        String[] partials = timestamp.split(":");
        return (long) ((Long.parseLong(partials[3]) + (Long.parseLong(partials[2]) + Long.parseLong(partials[1])*60 +
                Long.parseLong(partials[0])*60*60) * Math.pow(10,6))/1000);
    }

    public long execTimeInMS(){
        if(!this.rollback)
            return (this.commitTimestampMS-this.startTimestampMS);
        else
            return -1;
    }


    public String getCommitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(String commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
        this.commitTimestampMS = this.timestampToMs(this.commitTimestamp);
    }

    public long getStartTimestampMS() {
        return startTimestampMS;
    }

    public void setStartTimestampMS(long startTimestampMS) {
        this.startTimestampMS = startTimestampMS;
    }

    public long getCommitTimestampMS() {
        return commitTimestampMS;
    }

    public void setCommitTimestampMS(long commitTimestampMS) {
        this.commitTimestampMS = commitTimestampMS;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Write> getWriteset() {
        return writeset;
    }

    public void setWriteset(List<Write> writeset) {
        this.writeset = writeset;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public boolean isRollback() {
        return rollback;
    }

    public void setRollback(boolean rollback) {
        this.rollback = rollback;
    }

    public void addWrite(Write write){
        writeset.add(write);
    }

    public long getStartTimestamMS() {
        return startTimestampMS;
    }

    @Override
    public String toString() {
        return "Entry{" +
                "id=" + id +
                ", startTimestamp='" + startTimestamp + '\'' +
                ", startTimestampMS=" + startTimestampMS +
                ", commitTimestamp='" + commitTimestamp + '\'' +
                ", commitTimestampMS=" + commitTimestampMS +
                ", type='" + type + '\'' +
                ", rollback=" + rollback +
                ", writeset=" + writeset +
                '}';
    }
}
