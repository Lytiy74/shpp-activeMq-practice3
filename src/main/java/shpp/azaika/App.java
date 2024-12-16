package shpp.azaika;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.pojo.UserPojo;
import shpp.azaika.util.CsvWriter;
import shpp.azaika.util.MessageHandler;
import shpp.azaika.util.PropertyManager;
import shpp.azaika.util.UserPojoGenerator;
import shpp.azaika.util.mq.Consumer;
import shpp.azaika.util.mq.Producer;

import javax.jms.JMSException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private static ExecutorService producersExecutor;
    private static ExecutorService consumerExecutor;
    private static ExecutorService writerExecutor;

    public static void main(String[] args) throws IOException, JMSException {
        StopWatch allProgramWatch = new StopWatch(true);
        if (args.length < 1) {
            logger.error("Please provide the number of messages to send as the first argument.");
            throw new IllegalArgumentException();
        }

        PropertyManager propertyManager = new PropertyManager("app.properties");
        String userName = propertyManager.getProperty("activemq.user");
        String userPassword = propertyManager.getProperty("activemq.pwd");
        String urlMq = propertyManager.getProperty("activemq.url");
        String destinationName = propertyManager.getProperty("activemq.queue");
        long durationMillis = Long.parseLong(propertyManager.getProperty("generation.duration"));

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(userName, userPassword, urlMq);

        int messageCount = Integer.parseInt(args[0]);
        int threadsProducer = Integer.parseInt(propertyManager.getProperty("threads_producer"));
        int threadsConsumer = Integer.parseInt(propertyManager.getProperty("threads_consumer"));

        List<Future<Integer>> producersFutureList = startProducers(threadsProducer,threadsConsumer, connectionFactory, destinationName, messageCount);

        List<Future<Integer>> consumersFutureList = startConsumers(threadsConsumer, connectionFactory, destinationName);

        shutdownExecutor(producersExecutor, "Producers", durationMillis, TimeUnit.MILLISECONDS);
        consumerExecutor.shutdown();
        writerExecutor.shutdown();

        int producedMessage = calculateSum(producersFutureList);
        int consumedMessage = calculateSum(consumersFutureList);

        logger.info("----------------------------PERFORMANCE----------------------------");
        logPerformance("Produced messages per second", allProgramWatch.taken(),producedMessage);
        logPerformance("Consumed messages per second", allProgramWatch.taken(),consumedMessage);
        logPerformance("Total Execution Time", allProgramWatch.stop(), messageCount);
    }

    public static int calculateSum(List<Future<Integer>> futureList) {
        return futureList.stream()
                .mapToInt(future -> {
                    try {
                        return future.get();
                    } catch (Exception e) {
                        logger.error("Error retrieving result from Future: " + e.getMessage());
                        return 0;
                    }
                })
                .sum();
    }

    private static void shutdownExecutor(ExecutorService executor, String name, long timeout, TimeUnit unit) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeout, unit)) {
                logger.warn("{} threads did not finish in time. Forcing shutdown.", name);
                executor.shutdownNow();
            } else {
                logger.info("{} shutdown successfully.", name);
            }
        } catch (InterruptedException e) {
            logger.error("Waiting for {} threads to terminate was interrupted.", name, e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static void logPerformance(String taskName, long durationMillis, int messageCount) {
        double durationSeconds = TimeUnit.MILLISECONDS.toSeconds(durationMillis);
        double messagesPerSecond = messageCount / (durationSeconds > 0 ? durationSeconds : 1);
        logger.info("{} completed in {} seconds ({} messages/second) ({} total messages)", taskName, durationSeconds, messagesPerSecond, messageCount);
    }


    private static List<Future<Integer>> startProducers(int threadCount, int consumers, ActiveMQConnectionFactory connectionFactory, String destinationName, int messagesToSend) throws JMSException {
        producersExecutor = Executors.newFixedThreadPool(threadCount);
        List<Future<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            Producer task = new Producer(connectionFactory, new UserPojoGenerator(), messagesToSend,consumers);
            task.connect(destinationName);
            futures.add(producersExecutor.submit(task));
        }
        return futures;
    }

    private static List<Future<Integer>> startConsumers(int threadCount, ActiveMQConnectionFactory connectionFactory, String destinationName) throws JMSException {
        consumerExecutor = Executors.newFixedThreadPool(threadCount);
        List<Future<Integer>> futures = new ArrayList<>();
        BlockingQueue<UserPojo> validQueue = new ArrayBlockingQueue<>(threadCount * 3000);
        BlockingQueue<UserPojo> invalidQueue = new ArrayBlockingQueue<>(threadCount * 3000);

        for (int i = 0; i < threadCount; i++) {
            ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
            Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

            Consumer task = new Consumer(connectionFactory, new MessageHandler(objectMapper, validator, validQueue, invalidQueue));
            task.connect(destinationName);
            futures.add(consumerExecutor.submit(task));
        }
        startWriters(validQueue,invalidQueue);
        return futures;
    }

    private static void startWriters(BlockingQueue<UserPojo> validQueue, BlockingQueue<UserPojo> invalidQueue) {
        writerExecutor = Executors.newFixedThreadPool(2);

        writerExecutor.submit(() -> {
            try (CsvWriter validWriter = new CsvWriter("valid_users.csv")) {
                while (true) {
                    UserPojo userPojo = validQueue.poll();
                    if (userPojo == null && consumerExecutor.isTerminated()) break;
                    if (userPojo != null) validWriter.write(userPojo);
                }
            } catch (IOException e) {
                logger.error("Error writing to valid_users.csv", e);
            }
        });

        writerExecutor.submit(() -> {
            try (CsvWriter invalidWriter = new CsvWriter("invalid_users.csv")) {
                while (true) {
                    UserPojo userPojo = invalidQueue.poll();
                    if (userPojo == null && consumerExecutor.isTerminated()) break;
                    if (userPojo != null) invalidWriter.write(userPojo);
                }
            } catch (IOException e) {
                logger.error("Error writing to invalid_users.csv", e);
            }
        });

    }

}
