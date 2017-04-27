package benchtpce.transaction;

import org.apache.omid.transaction.RollbackException;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionException;
import org.apache.omid.transaction.TransactionManager;

/**
 * Created by carlosmorais on 25/04/2017.
 */
public class TransactionManagerServiceOmid implements TransactionManagerService {

    private TransactionManager tm;

    public TransactionManagerServiceOmid(TransactionManager tm) {
        this.tm = tm;
    }

    @Override
    public TransactionService begin() {
        try {
            Transaction t = tm.begin();
            return new TransactionServiceOmid(t);
        } catch (TransactionException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void commit(TransactionService transaction) throws RollbackException, TransactionException {
        TransactionServiceOmid t = (TransactionServiceOmid) transaction;
        tm.commit(t.getTransaction());
    }


}
