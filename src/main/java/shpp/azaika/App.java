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
    private static final List<Producer> producers = new ArrayList<>();
    private static final List<Consumer> consumers = new ArrayList<>();


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
        connectionFactory.setTrustedPackages(List.of("shpp.azaika"));

        int messageCount = Integer.parseInt(args[0]);
        int threadsProducer = Integer.parseInt(propertyManager.getProperty("threads_producer"));
        int threadsConsumer = Integer.parseInt(propertyManager.getProperty("threads_consumer"));

        List<Future<Integer>> producersFutureList = startProducers(threadsProducer, connectionFactory, destinationName, messageCount);
        List<Future<Integer>> consumersFutureList = startConsumers(threadsConsumer, connectionFactory, destinationName);

        shutdownExecutor(producersExecutor, "Producers", durationMillis, TimeUnit.MILLISECONDS);
        producers.getFirst().sendPoisonPill(consumers.size());
        producers.forEach(Producer::close);

        shutdownExecutor(consumerExecutor, "Consumers", 1, TimeUnit.MINUTES);
        consumers.forEach(Consumer::close);
        writerExecutor.shutdown();

        int producedMessage = calculateSum(producersFutureList);
        int consumedMessage = calculateSum(consumersFutureList);

        logger.info("----------------------------PERFORMANCE----------------------------");
        logger.info("Produced messages {}", producedMessage);
        logger.info("Consumed messages {}", consumedMessage);
        logger.info("All task completed in {} seconds ({} messages/second) ({} total messages)", allProgramWatch.stop(), (producedMessage+consumedMessage)/allProgramWatch.taken(), messageCount);
    }

    public static int calculateSum(List<Future<Integer>> futureList) {
        return futureList.stream()
                .mapToInt(future -> {
                    try {
                        return future.get();
                    } catch (ExecutionException | InterruptedException e) {
                        logger.error("Error retrieving result from Future: {}", e.getMessage());
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

    private static List<Future<Integer>> startProducers(int producersQty, ActiveMQConnectionFactory connectionFactory, String destinationName, int messagesToSend) throws JMSException {
        producersExecutor = Executors.newFixedThreadPool(producersQty);
        int messagesPerThread = messagesToSend / producersQty;
        int pendingMessages = messagesToSend % producersQty;

        List<Future<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < producersQty; i++) {
            int messagesForThisThread = (i == producersQty - 1) ? messagesPerThread + pendingMessages : messagesPerThread;

            Producer producer = new Producer(connectionFactory, new UserPojoGenerator(), messagesForThisThread);
            producers.add(producer);
            producer.connect(destinationName);
            futures.add(producersExecutor.submit(producer));
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

            Consumer consumer = new Consumer(connectionFactory, new MessageHandler(objectMapper, validator, validQueue, invalidQueue));
            consumers.add(consumer);
            consumer.connect(destinationName);
            futures.add(consumerExecutor.submit(consumer));
        }
        startWriters(validQueue, invalidQueue);
        return futures;
    }

    private static void startWriters(BlockingQueue<UserPojo> validQueue, BlockingQueue<UserPojo> invalidQueue) {
        writerExecutor = Executors.newFixedThreadPool(2);

        writerExecutor.submit(() -> writeUsersToCsv(validQueue, "valid_users.csv"));
        writerExecutor.submit(() -> writeUsersToCsv(invalidQueue, "invalid_users.csv"));
    }

    private static void writeUsersToCsv(BlockingQueue<UserPojo> queue, String fileName) {
        try (CsvWriter writer = new CsvWriter(fileName)) {
            while (true) {
                UserPojo userPojo = queue.poll();
                if (userPojo == null && consumerExecutor.isTerminated()) break;
                if (userPojo != null) writer.write(userPojo);
            }
        } catch (IOException e) {
            logger.error("Error writing to file: {}", fileName, e);
        }
    }


}
