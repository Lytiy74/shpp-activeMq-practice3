package shpp.azaika;

import shpp.azaika.util.PropertyManager;
import shpp.azaika.util.UserPojoGenerator;
import shpp.azaika.util.mq.Consumer;
import shpp.azaika.util.mq.Producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class App {
    public static void main(String[] args) throws InterruptedException, IOException {
        PropertyManager propertyManager = new PropertyManager("app.properties");

        List<Producer> producers = new ArrayList<>();
        List<Consumer> consumers = new ArrayList<>();
        List<Thread> threadArrayList = new ArrayList<>();

        int msgs = Integer.parseInt(System.getProperty("N"));
        int threads = Integer.parseInt(System.getProperty("threads"));

        int msgsPerThread = msgs / threads;
        int remainingMsgs = msgs % threads;

        for (int i = 0; i < threads; i++) {
            int msgsForThisThread = msgsPerThread + (i < remainingMsgs ? 1 : 0);

            producers.add(createProducer(propertyManager, msgsForThisThread));
            threadArrayList.add(startThread(producers.get(i)));

            consumers.add(createConsumer(propertyManager));
            threadArrayList.add(startThread(consumers.get(i)));
        }

        int millis = Integer.parseInt(propertyManager.getProperty("generation.duration"));
        Thread.sleep(millis);

        stopProducers(producers);

        joinThreads(threadArrayList);
    }

    private static Producer createProducer(PropertyManager propertyManager, int msgs) {
        return new Producer(propertyManager, new UserPojoGenerator(), msgs);
    }

    private static Consumer createConsumer(PropertyManager propertyManager) {
        return new Consumer(propertyManager);
    }

    private static void stopProducers(List<Producer> producers) {
        producers.forEach(Producer::stop);
    }

    private static void joinThreads(List<Thread> threads) throws InterruptedException {
        for (Thread thread : threads) {
            thread.join();
        }
    }

    private static Thread startThread(Runnable task) {
        Thread thread = new Thread(task);
        thread.start();
        return thread;
    }
}
