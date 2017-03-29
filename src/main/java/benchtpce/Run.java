package benchtpce;

import benchtpce.metrics.BenchmarkMetrics;
import benchtpce.metrics.TraceMetrics;
import benchtpce.runners.Runner;
import benchtpce.common.TpcConfig;

import benchtpce.trace.*;
import benchtpce.tpce.HBaseTables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import benchtpce.parser.Parser;

import javax.swing.*;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;


/**
 * Created by carlosmorais on 24/02/2017.
 */
public class Run {

    private static final Logger logger = LoggerFactory.getLogger(Run.class);

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        LocalTime now = LocalTime.now();
        String folderName = now.getHour()+"_"+now.getMinute();

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
        List<Future<TraceMetrics>> resultList = new ArrayList<>();
        for (Trace trace : traceList) {
            Runner runner = new Runner(trace, tpcConfig, folderName);
            Future<TraceMetrics> result = executor.submit(runner);
            resultList.add(result);
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        BenchmarkMetrics benchmarkMetrics = new BenchmarkMetrics(folderName, "benchmark");
        List<TraceMetrics> list = new ArrayList<>();
        for (Future<TraceMetrics> traceMetricsFuture : resultList) {
            list.add(traceMetricsFuture.get());
        }
        benchmarkMetrics.calcMetrics(list);

        logger.info("The benchmark is finished.");
        System.exit(0);

    }

}
