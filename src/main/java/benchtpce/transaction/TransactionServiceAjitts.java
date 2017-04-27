package benchtpce.transaction;

import client.TransactionAjitts;

/**
 * Created by carlosmorais on 25/04/2017.
 */
public class TransactionServiceAjitts implements TransactionService {
    TransactionAjitts transaction;

    public TransactionServiceAjitts(TransactionAjitts transaction) {
        this.transaction = transaction;
    }

    public TransactionAjitts getTransaction() {
        return transaction;
    }

    public void setTransaction(TransactionAjitts transaction) {
        this.transaction = transaction;
    }

    @Override
    public long startTS() {
        return transaction.getBeginTS();
    }

    @Override
    public long commitTS() {
        return transaction.getCommitTS();
    }

    @Override
    public long id() {
        return transaction.getCommitTS();
    }
}
