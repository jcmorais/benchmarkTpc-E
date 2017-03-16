package benchtpce.tpce;

import benchtpce.runners.Runner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * Created by carlosmorais on 19/11/2016.
 */
public class HBaseTables {
    private static final Logger logger = LoggerFactory.getLogger(HBaseTables.class);

    public void deleteTables(Configuration config){
        TableName tableName;

        try {
            Connection conn = ConnectionFactory.createConnection(config);
            Admin admin = conn.getAdmin();
            TpcE tpcE = new TpcE();

            //first delete the tables if they exist
            for (String t : tpcE.tablesColumns.keySet()) {
                tableName = TableName.valueOf(t);
                if(admin.tableExists(tableName)){

                    try {
                        admin.disableTable(tableName);
                    } catch (TableNotEnabledException e) {
                    }
                    admin.deleteTable(tableName);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTables(Configuration config){
        TableName tableName;
        HColumnDescriptor family;
        HTableDescriptor table;

        try {
            logger.info("Creating the Tpc-E tables in HBase...");
            this.deleteTables(config);

            Connection conn = ConnectionFactory.createConnection(config);
            Admin admin = conn.getAdmin();
            TpcE tpcE = new TpcE();

            for (String t : tpcE.tablesColumns.keySet()) {
                tableName = TableName.valueOf(t);
                family = new HColumnDescriptor(TpcE.family);
                table = new HTableDescriptor(tableName);
                table.addFamily(family);
                admin.createTable(table);
            }
            logger.info("Tables created successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    /*
    public void printAllTables(Configuration config) throws IOException {
        TpcE tpcE = new TpcE();
        TableName tableName = TableName.valueOf(tpcE.TRADE);;
        Connection conn = null;
        Table table = null;

        try {
            conn = ConnectionFactory.createConnection(config);
            table = conn.getTable(tableName);
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result row : scanner) {
                String v2 = Bytes.toString(row.getValue(Bytes.toBytes(tpcE.TRADE), tpcE.tradeColumns.get(2)));
                String v3 = Bytes.toString(row.getValue(Bytes.toBytes(tpcE.TRADE), tpcE.tradeColumns.get(3)));
                String key = Bytes.toString(row.getRow());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) table.close();
            if (conn != null) conn.close();
        }

    }
    */


}
