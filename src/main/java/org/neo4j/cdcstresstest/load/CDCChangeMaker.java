package org.neo4j.cdcstresstest.load;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

public class CDCChangeMaker {

    private final int numThreads;
    private final int batchSize;
    private final Driver driver;
    private boolean running = false;
    private LinkedList<Thread> threads = new LinkedList<>();
    private long totalRuntime;
    private int totalChanges;
    private final Random random = new Random();

    public CDCChangeMaker(String uri, AuthToken auth, int numThreads, int batchSize) {
        this.numThreads = numThreads;
        this.batchSize = batchSize;
        this.driver = GraphDatabase.driver(uri, auth);
        reset();
    }

    private synchronized void addChanges(int changes) {
        totalChanges += changes;
    }

    private void reset() {
        running = false;
        totalRuntime = 0;
        totalChanges = 0;
        threads = new LinkedList<>();
    }

    private void createNodes(Session session, int count, long threadId, int payloadBytes) {
        var createNodesCypher = """
            UNWIND range(1,$node_count) AS i
            CREATE (n:label_id_%s)
            SET n.payload = $payload
            """.stripIndent();
        var payload = new byte[payloadBytes];
        random.nextBytes(payload);
        session.run(createNodesCypher.formatted(Long.toString(threadId)), Map.of("node_count", count, "payload", payload)).consume();
    }

    private void deleteNodes(Session session, long threadId) {
        var deleteNodesCypher = """
            MATCH (n:label_id_%s)
            DETACH DELETE n
            """.stripIndent();
        session.run(deleteNodesCypher.formatted(Long.toString(threadId)), Map.of("label", threadId)).consume();
    }

    private void waitIfNecessary(long startTime, int changeRatePerThreadPerSecond, int changes) {
        var ratePerMs = changeRatePerThreadPerSecond / 1000.0;
        var elapsed = System.currentTimeMillis() - startTime;
        var expectedChanges = elapsed * ratePerMs;
        var excessChanges = changes - expectedChanges;
        if (excessChanges > 0) {
            var requiredDelay = excessChanges / ratePerMs;
            try {
                Thread.sleep((int)requiredDelay);
            } catch (InterruptedException e) {
                System.err.println("ChangeMaker interrupted while sleeping");
                Thread.currentThread().interrupt();
            }
        } else {
            //System.err.println("ChangeMaker not keeping up");
        }
    }

    private void changeMakerThread(Driver driver, int duration, int changeRatePerThreadPerSecond, int payloadBytes) {
        var startTime = System.currentTimeMillis();
        var changes = 0;
        try(var session = driver.session()) {
            do {
                createNodes(session, batchSize /2, Thread.currentThread().threadId(), payloadBytes);
                deleteNodes(session, Thread.currentThread().threadId());
                changes += batchSize;
                addChanges(batchSize);
                waitIfNecessary(startTime, changeRatePerThreadPerSecond, changes);
            }
            while (startTime + duration > System.currentTimeMillis());
        }
        var runtime = System.currentTimeMillis() - startTime;
        synchronized (this) {
            totalRuntime += runtime;
        }
    }

    public void start(int duration, int targetChangeRate, int payloadBytes) {
        if (running) {
            throw new IllegalStateException("Already running");
        }
        running = true;

        var changeRatePerThreadPerSecond = targetChangeRate / numThreads;
        for(int i = 0; i < numThreads; i++) {
            var thread = new Thread(() -> changeMakerThread(driver, duration, changeRatePerThreadPerSecond, payloadBytes));
            thread.setName("ChangeMakerThread-" + i);
            threads.add(thread);
        }
        for (var thread : threads) {
            thread.start();
        }
    }

    public ChangeMakerResult waitForResults() throws InterruptedException {
        for (var thread : threads) {
            thread.join();
        }
        var changes = totalChanges;
        var runtime = this.totalRuntime;
        reset();
        return new ChangeMakerResult(
                changes,
                runtime / (float) numThreads
        );
    }

    public record ChangeMakerResult(int totalChanges, float averageRuntime){}

}
