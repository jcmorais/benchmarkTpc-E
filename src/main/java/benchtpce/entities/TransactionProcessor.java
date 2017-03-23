package benchtpce.entities;

import benchtpce.common.ThreadCounter;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.omid.transaction.RollbackException;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by carlosmorais on 24/02/2017.
 */
public class TransactionProcessor implements Runnable{
    private TpcTransaction tpcTransaction;
    private TransactionManager tm;
    Connection conn;
    HConnection hconn;
    private ThreadCounter counter;

    private boolean transactionMode;

    private static final Logger LOG = LoggerFactory.getLogger(TransactionProcessor.class);

    public TransactionProcessor(TpcTransaction tx, HConnection hconn, TransactionManager tm, ThreadCounter counter) {
        this.tpcTransaction = tx;
        this.tm = tm;
        this.counter = counter;
        this.transactionMode = true;
        this.hconn = hconn;
    }

    public TransactionProcessor(TpcTransaction tpcTransaction, Connection conn, ThreadCounter counter) {
        this.tpcTransaction = tpcTransaction;
        this.conn = conn;
        this.counter = counter;
        this.transactionMode = false;
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
            Transaction  tx = null;
            if(transactionMode) {
                tx = tm.begin();
                tpcTransaction.setId(tx.getTransactionId());
                beginTime = System.currentTimeMillis()-start;
                tpcTransaction.setBeginTime(beginTime);
                LOG.debug("Tx:{} - begin done in {} ms",tx.getTransactionId(), beginTime);
            }


            //sleep the original duration
            // TODO: 03/03/2017 if a have a Beta ...
            try {
                long execTime = entry.execTimeInMS();
                if(execTime >0)
                    TimeUnit.MILLISECONDS.sleep(execTime);
                if(transactionMode){
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

            if(transactionMode) {
                tpcTransaction.omidTransaction(tx, hconn);
                workTime = System.currentTimeMillis()-start-beginTime-sleepTime;
                this.tpcTransaction.setWorkTime(workTime);
                LOG.debug("Tx:{} - work done in {} ms",tx.getTransactionId(), workTime);
            }
            else {
                tpcTransaction.noTrasaction(conn);
                workTime = System.currentTimeMillis()-start-sleepTime;
                this.tpcTransaction.setWorkTime(workTime);
            }
            try {
                if(transactionMode) {
                    tm.commit(tx);
                    long commitTime = System.currentTimeMillis()-start-beginTime-workTime-sleepTime;
                    tpcTransaction.setCommitTime(commitTime);
                    tpcTransaction.setExecTime(commitTime+beginTime+workTime+sleepTime);
                    tpcTransaction.setCommit(true);
                    LOG.debug("Tx:{} - commit done in {} ms",tx.getTransactionId(), workTime);
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
