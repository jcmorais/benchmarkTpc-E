package benchtpce.runners;

import benchtpce.tpce.HBaseTables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Created by carlosmorais on 23/03/2017.
 */
public class CreateHBaseTables {
    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        HBaseTables hbaseTables = new HBaseTables();
        hbaseTables.createTables(config);
        System.exit(0);
    }
}
