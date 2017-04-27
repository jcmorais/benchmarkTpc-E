package benchtpce.runners;

import benchtpce.common.ThreadCounter;
import benchtpce.common.TpcConfig;
import benchtpce.metrics.Metrics;
import benchtpce.metrics.TraceMetrics;
import benchtpce.trace.*;
import benchtpce.transaction.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.omid.transaction.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by carlosmorais on 24/02/2017.
 */
public class Runner implements Callable{
    private static final Logger logger = LoggerFactory.getLogger(Runner.class);

    private Trace trace;
    private TpcConfig tpcConfig;

    TransactionManagerService tm;

    HConnection conn;

    ScheduledExecutorService executorService;

    TraceMetrics metrics;


    public Runner(Trace trace, TpcConfig tpcConfig, String folderName, int number) {
        this.trace = trace;
        this.tpcConfig = tpcConfig;
        this.metrics = new TraceMetrics(folderName, trace.getFilename()+"_"+number);
    }


    public void run() throws IOException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        try {
            conn = HConnectionManager.createConnection(config);
        } catch (IOException e) {
            logger.error("fail to create HConnection: {}", e.getMessage());
        }


        switch (tpcConfig.getTransactionModeEnum()) {
            case OMID:
                HBaseOmidClientConfiguration omidClientConfiguration = new HBaseOmidClientConfiguration();
                TransactionManager tmOmid = HBaseTransactionManager.newInstance(omidClientConfiguration);
                tm = new TransactionManagerServiceOmid(tmOmid);
                break;
            case HBASE:
                break;
            case AJITTS:
                tm = new TransactionManagerServiceAjitts();
                break;
            default:
                break;
        }

        runTrace();
    }


    private void runTrace(){

        this.warm();

        ThreadCounter counter = new ThreadCounter();
        Iterator<Entry> it = trace.getEntrys().iterator();
        long currentTime = 0, startTimeEntry = 0, diff = 0, sleep=0;
        Entry entry;
        TransactionProcessor txProcessor=null;
        TpcTransaction tpcTx;
        List<TransactionProcessor> transactionsList = new ArrayList<>();
        List<ScheduledFuture<?>> futereList = new ArrayList<>();
        boolean first = true; // if is the first transaction
        boolean allTypeTransactions = tpcConfig.isAllTypeTransactions();
        List<String> allowedTx = tpcConfig.getAllowedTransactions();

        executorService = Executors.newScheduledThreadPool(tpcConfig.getThreadPool());

        //Metrics
        metrics.setStart(System.currentTimeMillis());


        //TpcTransaction Scheduling
        while (it.hasNext()) {
            entry = it.next();
            currentTime = System.currentTimeMillis();
            startTimeEntry = entry.getStartTimestamMS();

            if(allTypeTransactions && !allowedTx.contains(entry.getType()))
                continue;

            if(first) {
                diff = currentTime - startTimeEntry;
                first = false;
            }
            else
                sleep = diff + startTimeEntry - currentTime;

            tpcTx = new TpcTransaction(entry);
            tpcTx.setExpectedStartRun(diff + startTimeEntry);


            switch (tpcConfig.getTransactionModeEnum()) {
                case HBASE:
                    txProcessor = new TransactionProcessor(tpcTx, conn, counter);
                    break;
                case OMID:
                    txProcessor = new TransactionProcessor(tpcTx, conn, tm, counter);
                    break;
                case AJITTS:
                    txProcessor = new TransactionProcessor(tpcTx, conn, tm, counter);
                    break;
                default:
                    System.exit(0);
                    break;
            }

            executorService.schedule(txProcessor, sleep, TimeUnit.MILLISECONDS);
            transactionsList.add(txProcessor);
        }

        logger.info("shutdown executorService");
        executorService.shutdown();
        logger.info("wait for all threads of executorService");
        try {
            long timeout = diff+120000; //wait the time to start all transactions + 2minutes
            executorService.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("timeout exceded when await for termination of executorService");
        }
        logger.info("end of executorService work");

        metrics.setEnd(System.currentTimeMillis());
        metrics.setTotalTx(transactionsList.size());

        metrics.calcMetrics(transactionsList);
        logger.info(metrics.shortMetrics());

    }

    private void warm() {
        // TODO: 04/03/2017 fazer aqui uma fase de aquecimeto do bechmark
        //criaruma tabela no HBAse parafazer ums puts
        //usar todas as Threads da pool
        //esperar que tudo termine e arrancar com o benchmark
    }


    @Override
    public TraceMetrics call() throws IOException, InterruptedException {
        this.run();
        return metrics;
    }
}
