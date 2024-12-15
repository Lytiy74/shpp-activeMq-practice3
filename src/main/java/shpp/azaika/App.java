package shpp.azaika;

import com.fasterxml.jackson.core.JsonProcessingException;
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

/**
 * Main application class for managing message generation, sending, and processing using ActiveMQ.
 */
public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private static ExecutorService messageGeneratorExecutor;
    private static ExecutorService producersExecutor;
    private static ExecutorService consumerExecutor;
    private static ExecutorService writerExecutor;

    public static void main(String[] args) throws IOException, JMSException {
        StopWatch allProgramWatch = new StopWatch(true);
        if (args.length < 1) {
            logger.error("Please provide the number of messages to send as the first argument.");
            System.exit(1);
        }

        PropertyManager propertyManager = new PropertyManager("app.properties");
        String userName = propertyManager.getProperty("activemq.user");
        String userPassword = propertyManager.getProperty("activemq.pwd");
        String urlMq = propertyManager.getProperty("activemq.url");
        String destinationName = propertyManager.getProperty("activemq.queue");
        long durationMillis = Long.parseLong(propertyManager.getProperty("generation.duration"));

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(userName, userPassword, urlMq);

        int messageCount = Integer.parseInt(args[0]);
        int queueCapacity = 10_000;
        int threadCount = Integer.parseInt(propertyManager.getProperty("threads"));

        BlockingQueue<String> messageQueue = new ArrayBlockingQueue<>(queueCapacity);

        StopWatch generateMessagesStopWatch = new StopWatch(true);
        List<Future<Integer>> generateMessagesFutureList = generateMessages(messageCount, threadCount, messageQueue);

        StopWatch producersStopWatch = new StopWatch(true);
        List<Future<Integer>> producersFutureList = startProducers(threadCount, connectionFactory, messageQueue, destinationName);

        BlockingQueue<UserPojo> validQueue = new ArrayBlockingQueue<>(threadCount * 10);
        BlockingQueue<UserPojo> invalidQueue = new ArrayBlockingQueue<>(threadCount * 10);

        StopWatch consumersStopWatch = new StopWatch(true);
        List<Future<Integer>> consumersFutureList = startConsumers(threadCount, connectionFactory, destinationName, validQueue, invalidQueue);

        startWriters(validQueue, invalidQueue);

        shutdownExecutor(messageGeneratorExecutor, "Message Generator", durationMillis, TimeUnit.MILLISECONDS);
        shutdownExecutor(producersExecutor, "Producers", durationMillis, TimeUnit.MILLISECONDS);
        shutdownExecutor(consumerExecutor, "Consumers", durationMillis, TimeUnit.MILLISECONDS);
        shutdownExecutor(writerExecutor, "Writers", 1, TimeUnit.MINUTES);

        int generatedMessages = calculateSum(generateMessagesFutureList);
        int producedMessage = calculateSum(producersFutureList);
        int consumedMessage = calculateSum(consumersFutureList);

        logger.info("----------------------------PERFORMANCE----------------------------");
        logPerformance("Message Generation",generateMessagesStopWatch.stop(), generatedMessages);
        logPerformance("Message Sending (Producers)", producersStopWatch.stop(), producedMessage);
        logPerformance("Message Processing and Writing (Consumers/Writers)", consumersStopWatch.stop(), consumedMessage);
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

    private static List<Future<Integer>> generateMessages(int messageCount, int threadCount, BlockingQueue<String> messageQueue) {
        int messagesPerThread = messageCount / threadCount;
        int remainingMessages = messageCount % threadCount;
        List<Future<Integer>> futures = new ArrayList<>();

        messageGeneratorExecutor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            int messagesForThisThread = messagesPerThread + (i == threadCount - 1 ? remainingMessages : 0);
            futures.add(messageGeneratorExecutor.submit(createMessageGeneratorTask(messagesForThisThread, messageQueue)));
        }
        return futures;
    }

    private static List<Future<Integer>> startProducers(int threadCount, ActiveMQConnectionFactory connectionFactory, BlockingQueue<String> messageQueue, String destinationName) throws JMSException{
        producersExecutor = Executors.newFixedThreadPool(threadCount);
        List<Future<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            Producer task = new Producer(connectionFactory, messageQueue);
            task.connect(destinationName);
            futures.add(producersExecutor.submit(task));
        }
        return futures;
    }

    private static List<Future<Integer>> startConsumers(int threadCount, ActiveMQConnectionFactory connectionFactory, String destinationName, BlockingQueue<UserPojo> validQueue, BlockingQueue<UserPojo> invalidQueue) throws JMSException {
        consumerExecutor = Executors.newFixedThreadPool(threadCount);
        List<Future<Integer>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
            Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

            Consumer task = new Consumer(connectionFactory, new MessageHandler(objectMapper, validator, validQueue, invalidQueue));
            task.connect(destinationName);
            futures.add(consumerExecutor.submit(task));
        }
        return futures;
    }

    private static void startWriters(BlockingQueue<UserPojo> validQueue, BlockingQueue<UserPojo> invalidQueue) {
        writerExecutor = Executors.newFixedThreadPool(2);
        int timeoutSeconds = 3;

        writerExecutor.submit(() -> {
            try (CsvWriter validWriter = new CsvWriter("valid_users.csv")) {
                while (true) {
                    UserPojo userPojo = validQueue.poll(timeoutSeconds, TimeUnit.SECONDS);
                    if (userPojo == null) {
                        logger.info("Valid queue writer stopped after no data for {} seconds.", timeoutSeconds);
                        break;
                    }
                    validWriter.write(userPojo);
                }
            } catch (IOException e) {
                logger.error("Error writing to valid_users.csv", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Valid queue writer was interrupted.");
            }
        });

        writerExecutor.submit(() -> {
            try (CsvWriter invalidWriter = new CsvWriter("invalid_users.csv")) {
                while (true) {
                    UserPojo userPojo = invalidQueue.poll(timeoutSeconds, TimeUnit.SECONDS);
                    if (userPojo == null) {
                        logger.warn("Invalid queue writer stopped after no data for {} seconds.", timeoutSeconds);
                        break;
                    }
                    invalidWriter.write(userPojo);
                }
            } catch (IOException e) {
                logger.error("Error writing to invalid_users.csv", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Invalid queue writer was interrupted.");
            }
        });
    }

    private static Callable<Integer> createMessageGeneratorTask(int messageCount, BlockingQueue<String> messageQueue) {
        return () -> {
            int count = 0;
            logger.info("Starting message generation thread.");
            UserPojoGenerator userPojoGenerator = new UserPojoGenerator();
            ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

            for (int i = 0; i < messageCount; i++) {
                try {
                    UserPojo user = userPojoGenerator.generate();
                    messageQueue.put(objectMapper.writeValueAsString(user));
                    if (count % 1000 == 0) {
                    logger.debug("Generated messages {}", count);
                    }
                    count++;
                } catch (JsonProcessingException | InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Error during message generation", e);
                    break;
                }
            }

            logger.info("Finished message generation thread.");
            return count;
        };
    }

}
