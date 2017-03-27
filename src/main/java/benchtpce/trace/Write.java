package benchtpce.trace;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by carlosmorais on 19/11/2016.
 */
public class Write implements Serializable {

    public enum Statement {INSERT, UPDATE, DELETE}

    private String table;
    private Statement statement;

    private Map<Integer, String> oldValues; //<postion, oldValue>
    private Map<Integer, String> values; //<postion, newValue>

    public Write() {
        this.table = null;
        this.values = new HashMap<Integer, String>();
    }


    public Statement getStatement() {
        return statement;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }

    public Map<Integer, String> getOldValues() {
        return oldValues;
    }

    public void setOldValues(Map<Integer, String> oldValues) {
        this.oldValues = oldValues;
    }

    public Map<Integer, String> getValues() {
        return values;
    }

    public void setValues(Map<Integer, String> values) {
        this.values = values;
    }

    public void setValue(int position, String value) {
        this.values.put(position, value);
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public String toString() {
        return "Write{" +
                "table='" + table + '\'' +
                ", statement=" + statement +
                ", oldValues=" + oldValues +
                ", values=" + values +
                '}';
    }
}
