package benchtpce.common;

import org.apache.omid.YAMLUtils;

import java.io.IOException;
import java.util.List;

/**
 * Created by carlosmorais on 20/02/2017.
 */
public class TpcConfig {

    private static final String CONFIG_FILE_NAME = "tpc-config.yml";

    private boolean createTables;
    private boolean loadFromXML;
    private boolean loadFromSerialized;
    private boolean allTypeTransactions;
    private int threadPool;

    private String loadPath;
    private List<String> filesList;
    private List<String> allowedTransactions;

    private int replicateTrace;
    private long intervalBetweenReplicate;

    private String transactionMode;
    public enum TRANSACTION_MODE {
        HBASE,
        OMID,
        AJITTS
    };



    public TpcConfig() throws IOException {
        this(CONFIG_FILE_NAME);
    }

    TpcConfig(String configFileName) throws IOException {
        new YAMLUtils().loadSettings(configFileName, configFileName, this);
    }

    public TRANSACTION_MODE getTransactionModeEnum() {
        return TpcConfig.TRANSACTION_MODE.valueOf(transactionMode);
    }

    public String getTransactionMode() {
        return transactionMode;
    }

    public void setTransactionMode(String transactionMode) {
        this.transactionMode = transactionMode;
    }

    public boolean isAllTypeTransactions() {
        return allTypeTransactions;
    }

    public void setAllTypeTransactions(boolean allTypeTransactions) {
        this.allTypeTransactions = allTypeTransactions;
    }

    public List<String> getAllowedTransactions() {
        return allowedTransactions;
    }

    public void setAllowedTransactions(List<String> allowedTransactions) {
        this.allowedTransactions = allowedTransactions;
    }

    public boolean isCreateTables() {
        return createTables;
    }

    public void setCreateTables(boolean createTables) {
        this.createTables = createTables;
    }

    public boolean isLoadFromXML() {
        return loadFromXML;
    }

    public void setLoadFromXML(boolean loadFromXML) {
        this.loadFromXML = loadFromXML;
    }

    public boolean isLoadFromSerialized() {
        return loadFromSerialized;
    }

    public void setLoadFromSerialized(boolean loadFromSerialized) {
        this.loadFromSerialized = loadFromSerialized;
    }

    public int getThreadPool() {
        return threadPool;
    }

    public void setThreadPool(int threadPool) {
        this.threadPool = threadPool;
    }

    public String getLoadPath() {
        return loadPath;
    }

    public void setLoadPath(String loadPath) {
        this.loadPath = loadPath;
    }

    public List<String> getFilesList() {
        return filesList;
    }

    public void setFilesList(List<String> filesList) {
        this.filesList = filesList;
    }

    public int getReplicateTrace() {
        return replicateTrace;
    }

    public void setReplicateTrace(int replicateTrace) {
        this.replicateTrace = replicateTrace;
    }

    public long getIntervalBetweenReplicate() {
        return intervalBetweenReplicate;
    }

    public void setIntervalBetweenReplicate(long intervalBetweenReplicate) {
        this.intervalBetweenReplicate = intervalBetweenReplicate;
    }


    @Override
    public String toString() {
        return "TpcConfig{" +
                "createTables=" + createTables +
                ", loadFromXML=" + loadFromXML +
                ", loadFromSerialized=" + loadFromSerialized +
                ", allTypeTransactions=" + allTypeTransactions +
                ", threadPool=" + threadPool +
                ", loadPath='" + loadPath + '\'' +
                ", filesList=" + filesList +
                ", allowedTransactions=" + allowedTransactions +
                ", replicateTrace=" + replicateTrace +
                ", intervalBetweenReplicate=" + intervalBetweenReplicate +
                ", transactionMode='" + transactionMode + '\'' +
                '}';
    }
}
