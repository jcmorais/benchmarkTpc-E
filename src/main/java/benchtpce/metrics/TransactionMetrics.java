package benchtpce.metrics;

/**
 * Created by carlosmorais on 24/03/2017.
 */
public class TransactionMetrics {
    private String type; // Transaction type

    private int totalTx;
    private int abortsTx;
    private int commitTx;


    public TransactionMetrics(String type) {
        this.type = type;
        totalTx = 0;
        abortsTx = 0;
        commitTx = 0;
    }

    public void incAbort() {
        this.abortsTx++;
    }
    public void incAbort(int abortsTx) {
        this.abortsTx+=abortsTx;
    }

    public void incCommmit() {
        this.commitTx++;
    }

    public void getCommitTx(int commitTx) {
        this.commitTx*=commitTx;
    }

    public void incTotal() {
        this.totalTx++;
    }

    public void incTotal(int totalTx) {
        this.totalTx+=totalTx;
    }

    public int getTotalTx() {
        return totalTx;
    }

    public int getAbortsTx() {
        return abortsTx;
    }

    public int getCommitTx() {
        return commitTx;
    }

    public String getType() {
        return type;
    }

    public String shortMetrics() {

        StringBuilder sb = new StringBuilder();

        sb.append("Transaction: "+ type);
        sb.append(",  totalTx: "+totalTx);
        sb.append(", commitTx: "+commitTx);
        sb.append(", abortTx: "+abortsTx);
        if(abortsTx>0)
            sb.append(", abortRate: "+String.format("%.1f",(double)(abortsTx*100)/totalTx)+"%");
        else
            sb.append(", abortRate: 0%");
        sb.append(" ;\n\n");

        return sb.toString();
    }

    @Override
    public String toString() {
        return "TransactionMetrics{" +
                "type='" + type + '\'' +
                ", totalTx=" + totalTx +
                ", abortsTx=" + abortsTx +
                ", commitTx=" + commitTx +
                '}';
    }
}
