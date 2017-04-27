package benchtpce.transaction;

import org.apache.omid.transaction.Transaction;

/**
 * Created by carlosmorais on 25/04/2017.
 */
public class TransactionServiceOmid implements TransactionService{
    private Transaction transaction;

    public TransactionServiceOmid(Transaction transaction) {
        this.transaction = transaction;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public long startTS() {
        return 0;
    }

    @Override
    public long commitTS() {
        return 0;
    }

    @Override
    public long id() {
        return transaction.getTransactionId();
    }
}
