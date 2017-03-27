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
    private boolean transactions;
    private boolean allTypeTransactions;
    private int threadPool;

    private String loadPath;
    private List<String> filesList;
    private List<String> allowedTransactions;


    public TpcConfig() throws IOException {
        this(CONFIG_FILE_NAME);
    }

    TpcConfig(String configFileName) throws IOException {
        new YAMLUtils().loadSettings(configFileName, configFileName, this);
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

    public boolean isTransactions() {
        return transactions;
    }

    public void setTransactions(boolean transactions) {
        this.transactions = transactions;
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


    @Override
    public String toString() {
        return "TpcConfig{" +
                "createTables=" + createTables +
                ", loadFromXML=" + loadFromXML +
                ", loadFromSerialized=" + loadFromSerialized +
                ", transactions=" + transactions +
                ", threadPool=" + threadPool +
                ", loadPath='" + loadPath + '\'' +
                ", filesList=" + filesList +
                '}';
    }
}
