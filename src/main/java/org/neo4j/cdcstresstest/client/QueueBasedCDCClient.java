package org.neo4j.cdcstresstest.client;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class QueueBasedCDCClient implements CDCClient {

    private final Driver driver;

    private final Queue<Record> queue;
    private final int numReaderThreads;
    private Thread cdcTask;
    private final List<Thread> readerThreads;
    private int cdcThreadCount;
    private String changeId;
    private long startTime;
    private boolean interrupted;

    private List<Integer> processedPerThread;
    private List<Integer> sleepTimePerThread;
    private List<String> lastChangeIdPerThread;
    private List<Float> averageDurationPerThread;
    private List<Integer> cumulativeSizePerThread;
    private List<List<String>> changeIds;

    public QueueBasedCDCClient(String uri, AuthToken authToken, int numReaderThreads) {
        this.driver = GraphDatabase.driver(uri, authToken);
        this.queue = new ArrayBlockingQueue<>(100*1000);
        this.readerThreads = new LinkedList<>();
        this.numReaderThreads = numReaderThreads;
        reset();
    }

    private void fillQueue(String current) {
        changeId = current;
        var count = 0;
        var timeSpentIdle = 0;
        try (var session = driver.session()) {
            do {
                var lastCount = 0;
                var res = session.run(RETRIEVE_CHANGES_CYPHER, Map.of("previous_id", changeId));
                while (res.hasNext()) {
                    var next = res.next();
                    changeId = next.get("id").asString();
                    do {
                        try {
                            queue.offer(next);
                            break;
                        } catch (Exception e) {
                            System.err.println("Queue busy!" + queue.size());
                            Thread.sleep(100);
                        }
                    } while (true);
                    lastCount++;
                }

                if (lastCount < 100) {
                    timeSpentIdle += 100;
                    Thread.sleep(100);
                }
                count += lastCount;
            } while (!interrupted);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        synchronized (this){
            this.cdcThreadCount = count;
        }
    }

    private void readQueue() {
        var count = 0;
        var timeSlept = 0;
        var changeId = "";
        var averageDuration = 0.0f;
        var cumulativeSize = 0;
        var changeIds = new LinkedList<String>();
        do {
            var next = queue.poll();
            if (next != null) {
//                averageDuration = processRecord(next, averageDuration, count);
                cumulativeSize += next.toString().getBytes(StandardCharsets.UTF_8).length;
                changeId = next.get("id").asString();
                changeIds.add(changeId);
                count++;
            } else {
                try {
                    Thread.sleep(10);
                    timeSlept += 10;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        } while (!interrupted);
        synchronized (this) {
            lastChangeIdPerThread.add(changeId);
            processedPerThread.add(count);
            sleepTimePerThread.add(timeSlept);
            averageDurationPerThread.add(averageDuration);
            cumulativeSizePerThread.add(cumulativeSize);
            this.changeIds.add(changeIds);
        }
    }

    private float rollingAverage(float oldAverage, long newValue, int count) {
        if (count == 0) {
            return newValue;
        }
        return oldAverage * (count - 1) / count + newValue / (float) count;
    }

    /**
     * Emulate spending CPU time processing a record. This is intentionally wasteful, as to not give CPU time to other threads.
     * @param next Ignored
     * @param oldAverageDuration
     * @param count
     * @return The average duration 'wasted' in this method.
     */
    private float processRecord(Record next, float oldAverageDuration, int count) {
        var scale = 30;
        long sum = 0;
        var start = System.currentTimeMillis();
        for (int i = 1; i < scale; i++) {
            for (int j = 1; j < scale; j++) {
                for (int k = 1; k < scale; k++) {
                    // do some expensive n^3 operation
                    sum += (i*j)/k;
                }
            }
        }
        var duration = System.currentTimeMillis() - start;
        return rollingAverage(oldAverageDuration, duration, count);
    }

    private void reset() {
        this.lastChangeIdPerThread = new LinkedList<>();
        this.processedPerThread = new LinkedList<>();
        this.sleepTimePerThread = new LinkedList<>();
        this.averageDurationPerThread = new LinkedList<>();
        this.cumulativeSizePerThread = new LinkedList<>();
        this.changeIds = new LinkedList<>();
        this.interrupted = false;
    }

    @Override
    public void start(String current) {
        reset();
        cdcTask = new Thread(() -> fillQueue(current));
        cdcTask.setPriority(Thread.MAX_PRIORITY);
        cdcTask.setName("CDCClient from: " + current);
        cdcTask.start();
        for (int i = 0; i < numReaderThreads; i++) {
            var readTask = new Thread(this::readQueue);
            readerThreads.add(readTask);
            readTask.setName("CDCReader " + i + " for " + current);
            readTask.start();
        }
        startTime = System.currentTimeMillis();
    }

    @Override
    public RunResult waitForResults() throws InterruptedException {
        // Tell the threads to stop processing
        interrupted = true;

        // Wait for the threads to finish
        cdcTask.join();
        var cdcDuration = System.currentTimeMillis() - startTime;
        for (var thread : readerThreads) {
            thread.join();
        }
        var duration = System.currentTimeMillis() - startTime;

        System.out.println(cdcDuration + " : " + duration);

        // Verify that the reader threads don't process the same change multiple times:
//        for(var first : changeIds.getFirst()) {
//            for(int i = 1; i < changeIds.size(); i++) {
//                for(var last : changeIds.get(i)) {
//                    if (first.equals(last)) {
//                        System.err.println("First and last change id are the same: " + first);
//                    }
//                }
//            }
//        }

//        System.out.println(processedPerThread);
//        System.out.println(sleepTimePerThread);
//        System.out.println(lastChangeIdPerThread);
//        System.out.println(averageDurationPerThread);
//        var averageIdlePercentageForReaderThreads = processedPerThread.stream()
//                .mapToDouble(i -> i / (double)duration)
//                .average()
//                .orElse(0);
//        System.out.println(averageIdlePercentageForReaderThreads);
        var totalCount = processedPerThread.stream().reduce(Integer::sum).orElse(0);
        var totalSize = cumulativeSizePerThread.stream().reduce(Integer::sum).orElse(0);
        float averageRecordSize = totalSize / (float) totalCount;
        return new RunResult(cdcThreadCount, totalCount, duration, changeId, averageRecordSize);
    }
}
