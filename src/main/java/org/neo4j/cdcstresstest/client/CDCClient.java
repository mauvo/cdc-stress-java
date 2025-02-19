package org.neo4j.cdcstresstest.client;

public interface CDCClient {

    /**
     * Start querying from the given change id
     */
    void start(String current);

    /**
     * Tell the client threads to stop and wait for them to finish cleanly / record statistics
     */
    RunResult waitForResults() throws InterruptedException;

    /**
     * Stress test statistics
     *
     * @param numRecordsSeen      The number of records that have been received through db.cdc.query
     * @param numRecordsProcessed The number of records that have been "processed" by the client (emulated busy work, see {@link QueueBasedCDCClient#processRecord} for details).
     * @param duration            Wall-clock time of how long the client threads have been running (ms)
     * @param changeId            Last seen changeId by the client
     * @param averageRecordSize   Average size of the records seen in bytes (based on Record.toString, so includes "Record{" at the start.)
     */
    record RunResult(int numRecordsSeen, int numRecordsProcessed, long duration, String changeId, float averageRecordSize){}

    String RETRIEVE_CHANGES_CYPHER = """
        CALL db.cdc.query($previous_id)
        YIELD id, txId, seq, event, metadata
        RETURN id, txId, seq, event, metadata
        """.stripIndent();
}
