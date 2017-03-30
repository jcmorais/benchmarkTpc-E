package benchtpce.metrics;

import benchtpce.transaction.TpcTransaction;
import benchtpce.transaction.TransactionProcessor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.List;

/**
 * Created by carlosmorais on 29/03/2017.
 */
public class TraceMetrics extends Metrics {
    public TraceMetrics(String folderName, String name) {
        super(folderName, name);
    }




    public void calcMetrics(List<TransactionProcessor> transactionsList) {
        TpcTransaction tx;
        for (TransactionProcessor tp : transactionsList) {
            tx = tp.getTpcTransaction();

            transactionMetrics.get(tx.getType()).incTotal();
            delayTotal += tx.getDelay();

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
        tps();
        delayAvg();

        outPutMetrics(transactionsList);
    }


    private void outPutMetrics(List<TransactionProcessor> transactionsList)  {
        try  {
            LocalTime now = LocalTime.now();

            Path path = Paths.get("out/"+folderName+"/"+name+".txt");
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
}
