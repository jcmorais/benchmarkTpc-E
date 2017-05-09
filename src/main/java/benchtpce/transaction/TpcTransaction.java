package benchtpce.transaction;

import benchtpce.trace.Entry;
import benchtpce.trace.Write;
import client.TransactionAjitts;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.transaction.TTable;
import org.apache.omid.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import benchtpce.tpce.TpcE;
import transaction.TransactionService;
import transaction.TransactionServiceAjitts;
import transaction.TransactionServiceOmid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by carlosmorais on 24/02/2017.
 */
public class TpcTransaction {
    private static final Logger LOG = LoggerFactory.getLogger(TpcTransaction.class);

    private long id; //Tx id obtained from Sheduler

    private Entry entry;

    private String type; // type of TPC transaction

    private boolean transactionMode;
    private boolean commit;
    private boolean abort;

    // TODO: 13/03/2017 omid client don't provide the correct start/commit timestamp...
    private String commitTimestamp;
    private String startTimestamp;

    //To ensure the transaction starts at the right time
    private long expectedStartRun;
    private long startRun;

    private long beginTime;
    private long sleepTime;
    private long workTime;
    private long commitTime;

    private long execTime; //execTime = beginTime + sleepTime + workTime + commitTime

    public TpcTransaction(Entry entry) {
        this.type = entry.getType();
        this.execTime =-1;
        this.commit=false;
        this.abort =false;
        this.transactionMode = false;
        this.entry = entry;
    }

    public long getDelay() {
        return startRun-expectedStartRun;
    }

    public boolean isTransactionMode() {
        return transactionMode;
    }

    public void setTransactionMode(boolean transactionMode) {
        this.transactionMode = transactionMode;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getExpectedStartRun() {
        return expectedStartRun;
    }

    public void setExpectedStartRun(long expectedStartRun) {
        this.expectedStartRun = expectedStartRun;
    }

    public long getStartRun() {
        return startRun;
    }

    public void setStartRun(long startRun) {
        this.startRun = startRun;
    }

    public long getSleepTime() {
        return sleepTime;
    }

    public void setSleepTime(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCommitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(String commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public String getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(String startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(long beginTime) {
        this.beginTime = beginTime;
    }

    public long getWorkTime() {
        return workTime;
    }

    public void setWorkTime(long workTime) {
        this.workTime = workTime;
    }

    public long getCommitTime() {
        return commitTime;
    }

    public void setCommitTime(long commitTime) {
        this.commitTime = commitTime;
    }

    public long getExecTime() {
        return execTime;
    }

    public void setExecTime(long execTime) {
        this.execTime = execTime;
    }

    public boolean isCommit() {
        return commit;
    }

    public void setCommit(boolean commit) {
        this.commit = commit;
    }

    public boolean isAbort() {
        return abort;
    }

    public void setAbort(boolean abort) {
        this.abort = abort;
    }

    public Entry getEntry() {
        return entry;
    }

    public void setEntry(Entry entry) {
        this.entry = entry;
    }


    public void auxHBasePut(HConnection conn, String rowkey, String table, Map<Integer, String> writes) throws IOException {
        HTable t = null;
        try {
            t = (HTable) conn.getTable(table);
            Put put = new Put(Bytes.toBytes(rowkey));
            TpcE tpce = new TpcE();
            Map<Integer, byte[]> aux = tpce.tablesColumns.get(table);
            List<Integer> keys = tpce.tableKeys.get(table);

            for (Integer k : writes.keySet()) {
                if( !keys.contains(k) ) //Only puts if it does not belong to the primary key
                    put.add(tpce.family, aux.get(k), Bytes.toBytes(writes.get(k)));
            }

            if(put.size()>0)
                t.put(put);
        }
        finally {
            if (t!=null) t.close();
        }
    }


    private void auxHBaseDelete(HConnection conn, String rowkey, String table, Map<Integer, String> values) throws IOException {
        HTable t = null;
        try {
            t = (HTable) conn.getTable(table);
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            t.delete(delete);
        }
        finally {
            if (t!=null) t.close();
        }
    }



    public void auxOmidPut(Transaction tx, HConnection conn, String rowkey, String table, Map<Integer, String> writes) throws IOException {
        Put put = new Put(Bytes.toBytes(rowkey));
        TpcE tpce = new TpcE();
        Map<Integer, byte[]> aux = tpce.tablesColumns.get(table);
        List<Integer> keys = tpce.tableKeys.get(table);

        if (tx!=null){
            for (Integer k : writes.keySet()) {
                if( !keys.contains(k) ) //Only puts if it does not belong to the primary key
                    put.add(tpce.family, aux.get(k), Bytes.toBytes(writes.get(k)));
            }

            //TTable tTable = new TTable(table);
            TTable tTable = new TTable(conn.getTable(table), conn.getTable(table));
            tTable.put(tx, put);
        }
    }

    private void auxOmidDelete(Transaction tx, HConnection conn, String rowkey, String table) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        TpcE tpce = new TpcE();

        if (tx!=null){
            //TTable tTable = new TTable(table);
            TTable tTable = new TTable(conn.getTable(table), conn.getTable(table));
            tTable.delete(tx, delete);
        }
    }

    public String makeRowkey(List<Integer> keys, Map<Integer, String> values){
        List<String> list = new ArrayList();
        for (Integer key : keys)
            list.add(values.get(key));
        return String.join("_", list);
    }

    public void omidTransaction(Transaction tx, HConnection conn) throws IOException {

        TpcE tpcE = new TpcE();
        transactionMode = true;
        for (Write write : entry.getWriteset()) {
            Map<Integer, String> writeValues = write.getValues();
            String table = write.getTable();
            String rowkey = makeRowkey(tpcE.tableKeys.get(write.getTable()), writeValues);

            switch (write.getStatement()){
                case DELETE:
                    auxOmidDelete(tx, conn, rowkey, table);
                    break;

                case INSERT:
                    auxOmidPut(tx, conn, rowkey, table, writeValues);
                    break;

                case UPDATE:
                    auxOmidPut(tx, conn, rowkey, table, writeValues);
                    break;

                default:
                    break;
            }

        }
    }




    public void noTrasaction(HConnection conn) throws IOException {
        TpcE tpcE = new TpcE();
        for (Write write : entry.getWriteset()) {
            Map<Integer, String> writeValues = write.getValues();
            String table = write.getTable();
            String rowkey = makeRowkey(tpcE.tableKeys.get(write.getTable()), writeValues);

            switch (write.getStatement()){
                case DELETE:
                    auxHBaseDelete(conn, rowkey, table, writeValues);
                    break;

                case INSERT:
                    auxHBasePut(conn, rowkey, table, writeValues);
                    break;

                case UPDATE:
                    auxHBasePut(conn, rowkey, table, writeValues);
                    break;

                default:
                    break;
            }

        }
    }

    public void runTransaction(TransactionService tx, HConnection conn) {
        transactionMode = true;

        if (tx instanceof TransactionServiceAjitts){
            //TransactionServiceAjitts t = (TransactionServiceAjitts) tx;
            //ajittsTransaction(t.getTransaction());
            try {
                noTrasaction(conn);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else if(tx instanceof TransactionServiceOmid){
            TransactionServiceOmid t = (TransactionServiceOmid) tx;
            try {
                omidTransaction(t.getTransaction(), conn);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    private void ajittsTransaction(TransactionAjitts transaction) {
        TpcE tpcE = new TpcE();

        for (Write write : entry.getWriteset()) {
            Map<Integer, String> values = write.getValues();
            String rowkey = makeRowkey(tpcE.tableKeys.get(write.getTable()), values);

            switch (write.getStatement()){
                case DELETE:
                    auxAjittsDelete(transaction, rowkey, write.getTable());
                    break;

                case INSERT:
                    auxAjittsPut(transaction, rowkey, write.getTable(), values);
                    break;

                case UPDATE:
                    auxAjittsPut(transaction, rowkey, write.getTable(), values);
                    break;

                default:
                    break;
            }

        }

    }

    private void auxAjittsPut(TransactionAjitts transaction, String rowkey, String table, Map<Integer, String> values) {
        //TODO: 27/04/2017 implemets...
    }

    private void auxAjittsDelete(TransactionAjitts transaction, String rowkey, String table) {
        //TODO: 27/04/2017 implemets...
    }



}
