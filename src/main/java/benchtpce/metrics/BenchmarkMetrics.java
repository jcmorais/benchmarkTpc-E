package benchtpce.metrics;

import benchtpce.common.TpcConfig;
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
public class BenchmarkMetrics extends Metrics {

    private TpcConfig tpcConfig;

    public BenchmarkMetrics(String folderName, String name, TpcConfig tpcConfig) {
        super(folderName, name);
        this.tpcConfig = tpcConfig;
    }


    public void calcMetrics(List<TraceMetrics> metricsList) {
        TpcTransaction tx;
        boolean first = true;
        for (Metrics metric : metricsList) {

            if (first){
                first = false;
                start = metric.start;
                end = metric.end;
            }
            else {
                if (start >= metric.start)
                    start = metric.start;

                if (end >= metric.end)
                    end = metric.end;
            }

            abortsTx += metric.abortsTx;
            commitTx += metric.commitTx;
            totalTx += metric.totalTx;

            for (String type : metric.transactionMetrics.keySet()) {
                TransactionMetrics tm = transactionMetrics.get(type);
                TransactionMetrics tm2 = metric.transactionMetrics.get(type);
                tm.incAbort(tm2.getAbortsTx());
                tm.incCommit(tm2.getCommitTx());
                tm.incTotal(tm2.getTotalTx());
                transactionMetrics.put(type, tm);
            }

            execTimeTotal += metric.execTimeTotal;
        }
        average();

        tps();

        outPutMetrics();
    }


    private void outPutMetrics()  {
        try  {
            LocalTime now = LocalTime.now();

            Path path = Paths.get("out/"+folderName+"/"+name+".txt");
            Files.createDirectories(path.getParent());
            BufferedWriter writer = Files.newBufferedWriter(path);

            writer.write(shortMetrics());
            writer.write("logs: "+tpcConfig.getFilesList()+"\n\n");

            for (TransactionMetrics metrics : transactionMetrics.values()) {
                writer.write(metrics.shortMetrics());
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
