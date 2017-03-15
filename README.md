# BenchmarkTpc-E
Implementation that runs Tpc-E benchmark with [Omid] and [HBase].

# Build & Run
    mvn package

    mvn exec:exec
    
# Configuration
   - hbase-omid-client-config.yml: To configure the omid connection.
   - hbase-site.xml: To configure the HBase connection.
   - log4j.properties: To configure the proper logger level.
   - tpc-config.yml: To configure the benchmark parameters.
   
# Instructions for running   
to-do

# License
Licensed under the MIT license.
   
    
[HBase]: https://github.com/apache/hbase
[Omid]: https://github.com/yahoo/omid
 
