package benchtpce.transaction;

import org.apache.omid.transaction.RollbackException;
import org.apache.omid.transaction.TransactionException;

/**
 * Created by carlosmorais on 25/04/2017.
 */
public interface TransactionManagerService {
    TransactionService begin();
    void commit(TransactionService transaction) throws RollbackException, TransactionException;
}
