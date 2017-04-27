package benchtpce.transaction;

import client.*;
import client.TransactionAjitts;

/**
 * Created by carlosmorais on 25/04/2017.
 */
public class TransactionManagerServiceAjitts implements TransactionManagerService {
    private TransactionManagerAjitts tm;

    public TransactionManagerServiceAjitts() {
        this.tm = new TransactionManagerAjittsImpl();
    }

    public TransactionManagerAjitts getTm() {
        return tm;
    }

    public void setTm(TransactionManagerAjitts tm) {
        this.tm = tm;
    }

    @Override
    public TransactionService begin() {
        TransactionAjitts t = tm.begin();
        return new TransactionServiceAjitts(t);
    }

    @Override
    public void commit(TransactionService transaction) {
        TransactionServiceAjitts t = (TransactionServiceAjitts) transaction;
        tm.commit(t.getTransaction());
    }
}
