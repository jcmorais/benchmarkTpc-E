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
public class BenchMetrics {
    private long start; //start time in MS
    private long end; //end time in MS

    private int totalTx;
    private int abortsTx;
    private int commitTx;

    private long execTimeTotal; //Sum of the duration of all transactions
    private Double execTimeAvg; //Avg execution time of the transactions
    private Double tps; //transactions per second

    private String name; //the name of de input/output file

    private Map<String, TransactionMetrics> transactionMetrics;


    public BenchMetrics(String name) {
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


    private void average(){
        execTimeAvg =  (((double) execTimeTotal /(double) totalTx)/(double)1000);
    }

    private void tps(int size) {
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


    public void calcMetrics(List<TransactionProcessor> transactionsList) {
        TpcTransaction tx;
        for (TransactionProcessor tp : transactionsList) {
            tx = tp.getTpcTransaction();
            transactionMetrics.get(tx.getType()).incTotal();
            if(!tx.isTransactionMode())
                execTimeTotal += tx.getExecTime();
            else if(tx.isCommit() ) {
                execTimeTotal += tx.getExecTime();
                incCommmit();
                transactionMetrics.get(tx.getType()).incCommmit();
            }
            else if (tx.isAbort()) {
                incAbort();
                transactionMetrics.get(tx.getType()).incAbort();
            }
        }
        average();
        tps(transactionsList.size());

        outPutMetrics(transactionsList);
    }


    private void outPutMetrics(List<TransactionProcessor> transactionsList)  {
        try  {
            LocalTime now = LocalTime.now();

            Path path = Paths.get("out/"+name+"_"+now.getHour()+":"+now.getMinute()+".txt");
            Files.createDirectories(path.getParent());
            BufferedWriter writer = Files.newBufferedWriter(path);

            writer.write(shortMetrics());

            for (TransactionMetrics metrics : transactionMetrics.values()) {
                writer.write(metrics.shortMetrics());
            }

            for (TransactionProcessor transactionProcessor : transactionsList) {
                TpcTransaction t = transactionProcessor.getTpcTransaction();
                writer.write("Tx_"+(t.getId())+" ; " +
                        " begin:"+(t.getBeginTime()/ 1000.0)+"s," +
                        " sleep:"+(t.getSleepTime()/ 1000.0)+"s," +
                        " work:"+(t.getWorkTime()/ 1000.0)+"s," +
                        " commit:"+(t.getCommitTime()/ 1000.0)+"s  ; " +
                        " execTimeTotal: "+(t.getExecTime() / 1000.0)+"s" +
                        " log:"+(t.getEntry().execTimeInMS() / 1000.0)+"s ;" +
                        " delay:"+(t.getDelay())+"ms ;" +
                        "\n"
                )
                ;
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public double getTxAvg() {
        return this.execTimeAvg;
    }

    public String shortMetrics() {

        StringBuilder sb = new StringBuilder();

        sb.append("Logfile: "+name+" ;\n");
        sb.append("start: "+millisToReadbleString(start));
        sb.append(", end: "+millisToReadbleString(end));
        sb.append(" ;\n");
        sb.append("totalTx: "+totalTx);
        sb.append(", commitTx: "+commitTx);
        sb.append(", abortTx: "+abortsTx);
        sb.append(", abortRate: "+String.format("%.1f",(double)(abortsTx*100)/totalTx)+"%");
        sb.append(" ;\n");
        sb.append("AVG: "+String.format("%.3f",execTimeAvg)+"s\n");
        sb.append("TPS: "+String.format("%.3f",tps)+" tps");
        sb.append("\n\n");

        return sb.toString();
    }
}
