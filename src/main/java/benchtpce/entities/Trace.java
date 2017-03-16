package benchtpce.entities;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by carlosmorais on 25/11/2016.
 */
public class Trace implements Serializable{
    private Set<Entry> entrys;
    private String filename;

    public Trace() {
    }

    public Trace(Set<Entry> entrys) {
        this.entrys = entrys;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public Set<Entry> getEntrys() {
        return entrys;
    }

    public void setEntrys(Set<Entry> entrys) {
        this.entrys = entrys;
    }

    public long getSamllerTimestampInMS(){
        //todo Stream/ParallelStream
        long small = -1;
        boolean first = true;
        for (Entry entry : this.entrys) {
            long timestamp = entry.getStartTimestamMS();
            if(first){
                small = timestamp;
                first = false;
            }
            else if(small > timestamp)
                small = timestamp;
        }
        return small;
    }


    @Override
    public String toString() {
        return "Trace{" +
                "entrys=" + entrys +
                ", filename='" + filename + '\'' +
                '}';
    }
}
