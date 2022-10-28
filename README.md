## query-coordinator

An HTTP/REST microservice that analyzes SoQL queries and dispatches them to an execution engine.

## Running

Tests:

    sbt test

Query Coordinator service:

    bin/start_qc.sh

The above script will build an assembly if it is not present already.

If you are actively developing QC, it is probably better to start the QC from the SBT shell so you always have the latest copy of the code.

    sbt -Dconfig.file=configs/application.conf query-coordinator/run

## Unit Testing
### Query Rewriting
In order to test query rewriting you must know/build:
  * Dataset definitions, including
    * The dataset to column mappings
    * The column identifier to column name mappings
  * Rollup definitions
  * A query
  * The expected output, which will be:
    * An analyzed query
    * The rollup name that was used

You can find a sample test located here -> [QueryRewriterTest.scala](query-coordinator/src/test/scala/com/socrata/querycoordinator/QueryRewriterTest.scala)

The sample tests uses a static utility found here -> [QueryRewritingTestUtility.scala ](./query-coordinator/src/test/scala/com/socrata/querycoordinator/QueryRewritingTestUtility.scala)
