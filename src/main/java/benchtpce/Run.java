package benchtpce;

import benchtpce.runners.Runner;
import benchtpce.common.TpcConfig;

import benchtpce.entities.*;
import benchtpce.tpce.HBaseTables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import benchtpce.parser.Parser;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * Created by carlosmorais on 24/02/2017.
 */
public class Run {

    private static final Logger logger = LoggerFactory.getLogger(Run.class);

    public static void main(String[] args) throws IOException, InterruptedException {

        //benchmark configuration
        TpcConfig tpcConfig = new TpcConfig();
        logger.debug(tpcConfig.toString());


        //Traceload
        Parser parser = new Parser();
        List<Trace> traceList = parser.loadTrace(tpcConfig);

        if(tpcConfig.isCreateTables()) {
            Configuration config = HBaseConfiguration.create();
            HBaseTables hbaseTables = new HBaseTables();
            hbaseTables.createTables(config);
        }

        // TODO: 21/02/2017 put all entry keys in HBase before benchmark?


        //benchtpce.Run
        logger.info("run benchmark with {} logs", tpcConfig.getFilesList());
        ExecutorService executor = Executors.newFixedThreadPool(traceList.size());
        for (Trace trace : traceList) {
            executor.submit(() -> {
                Runner runner = new Runner(trace, tpcConfig);
                runner.run();
            });
        }


        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        logger.info("The benchmark is finished.");
        System.exit(0);

    }

}
