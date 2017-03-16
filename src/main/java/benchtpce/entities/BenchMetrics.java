package benchtpce.entities;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.List;

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

    private String name; //the name of de in/out file


    public BenchMetrics(String name) {
        this.name = name;
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
            if(!tx.isTransactionMode())
                execTimeTotal += tx.getExecTime();
            else if(tx.isCommit() ) {
                execTimeTotal += tx.getExecTime();
                incCommmit();
            }
            else if (tx.isAbort())
                incAbort();
        }
        average();

        outPutMetrics(transactionsList);
    }

    private void outPutMetrics(List<TransactionProcessor> transactionsList)  {
        try  {
            LocalTime now = LocalTime.now();

            Path path = Paths.get("out/"+name+"_"+now.getHour()+":"+now.getMinute()+".txt");
            Files.createDirectories(path.getParent());
            BufferedWriter writer = Files.newBufferedWriter(path);

            writer.write(shortMetrics());

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

        sb.append("start: "+millisToReadbleString(start));
        sb.append(", end: "+millisToReadbleString(end));
        sb.append(" ;\n");
        sb.append("totalTx: "+totalTx);
        sb.append(", commitTx: "+commitTx);
        sb.append(", abortTx: "+abortsTx);
        sb.append(", abortRate: "+String.format("%.1f",(double)(abortsTx*100)/totalTx)+"%");
        sb.append(" ;\n");
        sb.append("AVG(tx):"+String.format("%.3f",execTimeAvg)+"s");
        sb.append(" ;\n\n");

        return sb.toString();
    }
}
