package benchtpce.runners;

import benchtpce.common.ThreadCounter;
import benchtpce.common.TpcConfig;
import benchtpce.entities.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.omid.transaction.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import benchtpce.tpce.HBaseTables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by carlosmorais on 24/02/2017.
 */
public class Runner implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(Runner.class);

    private Trace trace;
    private TpcConfig tpcConfig;

    TransactionManager tm;

    Connection conn;
    HConnection hconn;

    ScheduledExecutorService executorService;

    BenchMetrics benchMetrics;


    public Runner(Trace trace, TpcConfig tpcConfig) {
        this.trace = trace;
        this.tpcConfig = tpcConfig;
        this.benchMetrics = new BenchMetrics(trace.getFilename());
    }



    @Override
    public void run(){
        if(tpcConfig.isTransactions())
            runOmidTransactions();
        else
            runHbase();
    }



    public void runOmidTransactions(){
        try {
            HBaseOmidClientConfiguration omidClientConfiguration = new HBaseOmidClientConfiguration();
            tm = HBaseTransactionManager.newInstance(omidClientConfiguration);
            hconn = HConnectionManager.createConnection(HBaseConfiguration.create());
        } catch (Exception e) {
            logger.error("fail load omidClientConfiguration: {}", e.getMessage());
        }

        this.runTrace();
    }


    public void runHbase(){
        try {
            Configuration config = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.runTrace();
    }


    private void runTrace(){

        this.warm();

        ThreadCounter counter = new ThreadCounter();
        Iterator<Entry> it = trace.getEntrys().iterator();
        long currentTime = 0, startTimeEntry = 0, diff = 0, time = 0, sleep=0;
        Entry entry = new Entry();
        TransactionProcessor txProcessor;
        TpcTransaction tpcTx;
        List<TransactionProcessor> transactionsList = new ArrayList<>();
        List<ScheduledFuture<?>> futereList = new ArrayList<>();
        boolean first = true; // if is the first transaction

        executorService = Executors.newScheduledThreadPool(tpcConfig.getThreadPool());

        //Metrics
        benchMetrics.setTotalTx(trace.getEntrys().size());
        benchMetrics.setStart(System.currentTimeMillis());


        //TpcTransaction Scheduling
        while (it.hasNext()) {
            entry = it.next();
            currentTime = System.currentTimeMillis();
            startTimeEntry = entry.getStartTimestamMS();

            if(first) {
                diff = currentTime - startTimeEntry;
                first = false;
            }
            else
                sleep = diff + startTimeEntry - currentTime;

            tpcTx = new TpcTransaction(entry);
            tpcTx.setExpectedStartRun(diff + startTimeEntry);

            if(tpcConfig.isTransactions())
                txProcessor = new TransactionProcessor(tpcTx, hconn, tm, counter);
            else
                txProcessor = new TransactionProcessor(tpcTx, conn,  counter);

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

        benchMetrics.setEnd(System.currentTimeMillis());

        benchMetrics.calcMetrics(transactionsList);
        logger.info(benchMetrics.shortMetrics());

    }

    private void warm() {
        // TODO: 04/03/2017 fazer aqui uma fase de aquecimeto do bechmark
        //criaruma tabela no HBAse parafazer ums puts
        //usar todas as Threads da pool
        //esperar que tudo termine e arrancar com o benchmark
    }


}
