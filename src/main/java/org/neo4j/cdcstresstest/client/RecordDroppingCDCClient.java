package org.neo4j.cdcstresstest.client;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.TransientException;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class RecordDroppingCDCClient implements CDCClient {

    Driver driver;
    private Thread thread;
    private long startTime;
    private ChangeCaptureRunnable task;

    public RecordDroppingCDCClient(String uri, AuthToken authToken) {
        this.driver = GraphDatabase.driver(uri, authToken);
    }

    @Override
    public void start(String current) {
        task = new ChangeCaptureRunnable(current);
        this.thread = new Thread(task);
        thread.setPriority(Thread.MAX_PRIORITY);
        thread.setName("CDCClient from: " + current);
        this.startTime = System.currentTimeMillis();
        this.thread.start();
    }

    @Override
    public RunResult waitForResults() throws InterruptedException {
        task.interrupt();
        thread.join();
        long endTime = System.currentTimeMillis();
        float averageRecordSize = task.cumulativeSize / (float) task.myCount;
        return new RunResult(task.myCount, 0, endTime-startTime, task.myChangeId, averageRecordSize);
    }


    private class ChangeCaptureRunnable implements Runnable {
        private boolean interrupted = false;
        private int myCount = 0;
        private long cumulativeSize = 0;
        private String myChangeId;
        public ChangeCaptureRunnable(String from) {
            super();
            this.myChangeId = from;
        }

        public void interrupt() {
            interrupted = true;
        }

        @Override
        public void run() {
            while (!interrupted) {
                var res = driver.session().run(RETRIEVE_CHANGES_CYPHER, Map.of("previous_id", myChangeId));
                try{
                    while (res.hasNext()) {
                        var next = res.next();
                        myCount += 1;
                        myChangeId = next.get("id").asString();
                        cumulativeSize += next.toString().getBytes(StandardCharsets.UTF_8).length;
                    }
                }
                catch (TransientException e) {
                    System.err.println("Ignoring transient exception at " + myChangeId + ": " + e.getMessage());
                }
            }
        }
    }
}
