package benchtpce.transaction;

import benchtpce.common.ThreadCounter;
import benchtpce.common.TpcConfig;
import benchtpce.trace.Entry;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.omid.transaction.RollbackException;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.TransactionManagerService;
import transaction.TransactionManagerServiceAjitts;
import transaction.TransactionManagerServiceOmid;
import transaction.TransactionService;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by carlosmorais on 24/02/2017.
 */
public class TransactionProcessor implements Runnable{
    private TpcTransaction tpcTransaction;
    TransactionManagerService tm;
    HConnection conn;
    private ThreadCounter counter;

    private String transactionMode;
    private boolean useTM;

    private static final Logger LOG = LoggerFactory.getLogger(TransactionProcessor.class);


    public TransactionProcessor(TpcTransaction tpcTransaction, HConnection conn, ThreadCounter counter) {
        this.tpcTransaction = tpcTransaction;
        this.conn = conn;
        this.counter = counter;
        this.transactionMode = TpcConfig.TRANSACTION_MODE.HBASE.toString();;
        this.useTM=false;
        this.tm=null;
    }

    public TransactionProcessor(TpcTransaction tpcTx, HConnection conn, TransactionManagerService tm, ThreadCounter counter) {
        this.tpcTransaction = tpcTx;
        this.conn = conn;
        this.counter = counter;
        this.useTM=true;
        this.tm=tm;

        if(tm instanceof TransactionManagerServiceAjitts)
            this.transactionMode = TpcConfig.TRANSACTION_MODE.AJITTS.toString();
        else if (tm instanceof TransactionManagerServiceOmid)
            this.transactionMode = TpcConfig.TRANSACTION_MODE.OMID.toString();

    }

    public TpcTransaction getTpcTransaction() {
        return tpcTransaction;
    }

    @Override
    public void run() {
        try {
            long start = System.currentTimeMillis(), beginTime=0, sleepTime=0, workTime=0;
            tpcTransaction.setStartRun(start);
            Entry entry = tpcTransaction.getEntry();
            counter.increment();
            TransactionService tx = null;


            if(useTM) {
                tx = tm.begin();
                tpcTransaction.setId(tx.id());
                beginTime = System.currentTimeMillis()-start;
                tpcTransaction.setBeginTime(beginTime);
                LOG.debug("Tx:{} - begin done in {} ms",tx.id(), beginTime);
            }


            //sleep the original duration
            // TODO: 03/03/2017 if a have a Beta ...
            try {
                long execTime = entry.execTimeInMS();
                if(execTime >0)
                    TimeUnit.MILLISECONDS.sleep(execTime);
                if(useTM){
                    sleepTime = System.currentTimeMillis()-start-beginTime;
                    tpcTransaction.setSleepTime(sleepTime);
                }
                else{
                    sleepTime = System.currentTimeMillis()-start;
                    tpcTransaction.setSleepTime(sleepTime);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if(useTM) {
                tpcTransaction.runTransaction(tx, conn);
                workTime = System.currentTimeMillis()-start-beginTime-sleepTime;
                this.tpcTransaction.setWorkTime(workTime);
                LOG.debug("Tx:{} - work done in {} ms",tx.id(), workTime);
            }
            else {
                tpcTransaction.noTrasaction(conn);
                workTime = System.currentTimeMillis()-start-sleepTime;
                this.tpcTransaction.setWorkTime(workTime);
                LOG.debug("Tx(HBase) work done in {} ms", workTime);
            }
            try {
                if(useTM) {
                    tm.commit(tx);
                    long commitTime = System.currentTimeMillis()-start-beginTime-workTime-sleepTime;
                    tpcTransaction.setCommitTime(commitTime);
                    tpcTransaction.setExecTime(commitTime+beginTime+workTime+sleepTime);
                    tpcTransaction.setCommit(true);
                    LOG.debug("Tx:{} - commit done in {} ms",tx.id(), workTime);
                }
                else
                    tpcTransaction.setExecTime(System.currentTimeMillis()-start);
            } catch (RollbackException e) {
                tpcTransaction.setAbort(true);
                LOG.debug("TpcTransaction {} ROLLBACK : {}", tx, e.getMessage());
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOG.debug("PANICK!!!");
        }
        this.counter.decrement();
    }
}
