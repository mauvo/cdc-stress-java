package org.neo4j.cdcstresstest.profiling;

import org.neo4j.cdcstresstest.client.CDCClient;
import org.neo4j.cdcstresstest.client.QueueBasedCDCClient;
import org.neo4j.cdcstresstest.load.CDCChangeMaker;
import org.neo4j.driver.*;

public class CDCStressTest {

    public static void main(String[] args) throws InterruptedException {
        // Connect: uri/username/password
        // Load: rate, numThreads, batchSize
        // Client: numThreads, cyphertext
        // Test: testTime / rate / rate increase

        String uri = System.getenv("NEO4J_URI");
        String username = System.getenv("NEO4J_USERNAME");
        String password = System.getenv("NEO4J_PASSWORD");

        System.out.println("uri: " + uri);
        System.out.println("username: " + username);
        AuthToken authToken = AuthTokens.basic(username, password);

        var cm = new CDCChangeMaker(uri, authToken, 10, 5000);
//        var cdc = new StatisticsOnlyCDCClient(uri, authToken);
        var cdc = new QueueBasedCDCClient(uri, authToken, 4);

        // Profiling around the localhost <=> aura limit of 20k changes per second
        // This limit seems to be mainly influenced by the network speed
        profile(uri, authToken, cdc, cm, 5*1000, 5*1000, 30*1000);

        // profiling around the localhost <=> localhost limit of 200k changes per second
//        profile(uri, authToken, cdc, cm, 150*1000, 10*1000, 250*1000);

        // one-off test
//        System.out.println(TestResult.headers());
//        var testResult = performanceTest(uri, authToken, cdc, cm, 190*1000, 30*1000);
//        System.out.println(testResult);
    }

    private static void profile(String uri, AuthToken authToken, CDCClient cdc, CDCChangeMaker cm, int initialRate, int rateIncrement, int finalRate) throws InterruptedException {
        var rate = initialRate;
        var testTime = 10*1000;
        System.out.println(TestResult.headers());
        while (rate <= finalRate) {
            var testResult = performanceTest(uri, authToken, cdc, cm, rate, testTime);
            System.out.println(testResult);
            rate += rateIncrement;
        }
    }

    private static void deleteAllNodes(String uri, AuthToken authToken) {
        try(var driver = GraphDatabase.driver(uri, authToken);
            var session = driver.session()) {
            session
                    .run("MATCH (n) DETACH DELETE n")
                    .consume();
        }
    }

    private static String currentChangeId(String uri, AuthToken authToken) {
        try(var driver = GraphDatabase.driver(uri, authToken);
            var session = driver.session()) {
            return session
                    .run("CALL db.cdc.current()")
                    .list().getFirst().get("id").asString();
        }
    }

    record TestResult(
            int targetChanges,
            int totalChanges,
            float totalCmTime,
            int totalCaptured,
            int totalProcessed,
            long totalCdcTime,
            float averageRecordSize,
            int rate,
            int testTime
    ) {
        private static final String template =
                "%15s,%15s,%15s,%15s,%15s,%15s,%15s,%15s,%15s,%15s";
        public static String headers() {
            return
                    template.formatted(
                            "Avg record size",
                            "Rate",
                            "Test time",
                            "Total changes",
                            "Total ChangeMake Time",
                            "Total changes seen by CDC",
                            "Total changes processed by CDC",
                            "Total time spent in CDC",
                            "Effective CDC download rate",
                            "Effective CDC process rate"
                    );
        }

        @Override
        public String toString() {
            return template.formatted(
                    averageRecordSize,
                    readable(rate, "ch/s"),
                    readable(testTime/1000, "s"),
                    readable(totalChanges, "chgs"),
                    readable(totalCmTime, "s"),
                    readable(totalCaptured, "chgs"),
                    readable(totalProcessed, "chgs"),
                    readable(totalCdcTime/1000.0, "s"),
                    readable(totalCaptured/(totalCdcTime/1000.0), "ch/s"),
                    readable(totalProcessed/(totalCdcTime/1000.0), "ch/s")
            );
        }
    }

    private static String readable(int value, String suffix) {
        return readable((double) value, suffix);
    }

    private static String readable(double value, String suffix) {
        if(value > 1000*1000) {
            return String.format("%.2fM %s", value/1000/1000, suffix);
        } else if(value > 1000) {
            return String.format("%.2fK %s", value/1000, suffix);
        } else {
            return String.format("%.2f %s", value, suffix);
        }
    }

    private static Object performanceTest(String uri, AuthToken authToken, CDCClient cdc, CDCChangeMaker cm, int rate, int testTime) throws InterruptedException {
        // Clean slate
        deleteAllNodes(uri, authToken);
        Thread.sleep(1000);
        var current = currentChangeId(uri, authToken);

        // Begin querying for changes & making changes
        cdc.start(current);
        cm.start(testTime, rate, 32);
        Thread.sleep(testTime);

        // Notify the client to stop and wait for the latest call to `db.cdc.query` to finish
        var cdcRes = cdc.waitForResults();
        // Notify the change maker to stop
        var cmRes = cm.waitForResults();

        // Report results
        var targetChanges = rate*testTime/1000;
        return new TestResult(
                targetChanges,
                cmRes.totalChanges(),
                cmRes.averageRuntime(),
                cdcRes.numRecordsSeen(),
                cdcRes.numRecordsProcessed(),
                cdcRes.duration(),
                cdcRes.averageRecordSize(),
                rate, testTime);
    }
}
