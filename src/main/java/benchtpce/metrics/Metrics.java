package benchtpce.metrics;

import benchtpce.tpce.TpcE;
import benchtpce.transaction.TpcTransaction;
import benchtpce.transaction.TransactionProcessor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by carlosmorais on 21/02/2017.
 */
public abstract class Metrics {
    protected long start; //start time in MS
    protected long end; //end time in MS

    protected int totalTx;
    protected int abortsTx;
    protected int commitTx;

    protected long execTimeTotal; //Sum of the duration of all transactions
    protected Double execTimeAvg; //Avg execution time of the transactions
    protected Double tps; //transactions per second

    protected String folderName;
    protected String name; //the name of de input/output file

    protected Map<String, TransactionMetrics> transactionMetrics;



    public Metrics(String folderName, String name) {
        this.folderName = folderName;
        this.name = name;
        transactionMetrics = new HashMap<>();
        transactionMetrics.put(TpcE.MARKET_FEED, new TransactionMetrics(TpcE.MARKET_FEED));
        transactionMetrics.put(TpcE.TRADE_RESULT, new TransactionMetrics(TpcE.TRADE_RESULT));
        transactionMetrics.put(TpcE.DATA_MAINTENANCE, new TransactionMetrics(TpcE.DATA_MAINTENANCE));
        transactionMetrics.put(TpcE.TRADE_ORDER, new TransactionMetrics(TpcE.TRADE_ORDER));
        transactionMetrics.put(TpcE.TRADE_UPDATE, new TransactionMetrics(TpcE.TRADE_UPDATE));
    }

    public long execInMS(){
        return end-start;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public int getTotalTx() {
        return totalTx;
    }

    public void setTotalTx(int totalTx) {
        this.totalTx = totalTx;
    }

    public int getAbortsTx() {
        return abortsTx;
    }

    public void setAbortsTx(int abortsTx) {
        this.abortsTx = abortsTx;
    }

    public int getCommitTx() {
        return commitTx;
    }

    public void setCommitTx(int commitTx) {
        this.commitTx = commitTx;
    }

    public Long duration() {
        return (start-end);
    }

    public void incAbort() {
        this.abortsTx++;
    }

    public void incCommmit() {
        this.commitTx++;
    }


    protected void average(){
        if(execTimeTotal > 0 && totalTx >0)
            execTimeAvg =  (((double) execTimeTotal /(double) totalTx)/(double)1000);
        else
            execTimeAvg = 0.0;
    }

    protected void tps() {
        tps = commitTx / (((double) end-start)/1000);
    }


    public String millisToReadbleString(long ms){

        long x = ms / 1000;
        long seconds = x % 60;
        x /= 60;
        long minutes = x % 60;
        x /= 60;
        long hours = x % 24;

        return hours+":"+minutes+":"+seconds;
    }


    public double getTxAvg() {
        return this.execTimeAvg;
    }

    public String shortMetrics() {

        StringBuilder sb = new StringBuilder();

        sb.append("Name: "+name+" ;\n");
        sb.append("start: "+millisToReadbleString(start));
        sb.append(", end: "+millisToReadbleString(end));
        sb.append(" ;\n");
        sb.append("totalTx: "+totalTx);
        sb.append(", commitTx: "+commitTx);
        sb.append(", abortTx: "+abortsTx);
        if(abortsTx>0)
            sb.append(", abortRate: "+String.format("%.1f",(double)(abortsTx*100)/totalTx)+"%");
        else
            sb.append(", abortRate: 0%");
        sb.append(" ;\n");
        sb.append("AVG: "+String.format("%.3f",execTimeAvg)+" s\n");
        sb.append("TPS: "+String.format("%.3f",tps)+" tps");
        sb.append("\n\n");

        return sb.toString();
    }
}
