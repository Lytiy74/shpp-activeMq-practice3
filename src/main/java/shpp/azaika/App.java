package shpp.azaika;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.util.PropertyManager;
import shpp.azaika.util.managers.ConsumerManager;
import shpp.azaika.util.managers.ExecutorServiceManager;
import shpp.azaika.util.managers.ProducerManager;
import shpp.azaika.util.managers.WriterManager;

import javax.jms.JMSException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

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

        ProducerManager producerManager = new ProducerManager(threadsProducer, threadsConsumer);
        producerManager.startProducers(connectionFactory, destinationName, threadsProducer, messageCount, durationMillis);

        ConsumerManager consumerManager = new ConsumerManager(threadsConsumer);
        consumerManager.startConsumers(connectionFactory,destinationName,threadsConsumer);

        WriterManager writerManager = new WriterManager();
        writerManager.startWriters(consumerManager.getValidQueue(), consumerManager.getInvalidQueue());

        ExecutorServiceManager.shutdownExecutor(producerManager.getExecutor(), "Producers", durationMillis, TimeUnit.MILLISECONDS);
        producerManager.shutdownProducers();

        ExecutorServiceManager.shutdownExecutor(consumerManager.getExecutor(), "Consumers", 10, TimeUnit.MINUTES);
        consumerManager.shutdownConsumers();
        writerManager.shutdownWriterExecutor();

        int producedMessages = producerManager.getProducedMessageCount();
        int consumedMessages = consumerManager.getConsumedMessageCount();
        long durationInSecond = TimeUnit.SECONDS.convert(allProgramWatch.stop(), TimeUnit.MILLISECONDS);
        logger.info("------------PERFORMANCE------------");
        logger.info("**Produced messages {}", producedMessages);
        logger.info("**Consumed messages {}", consumedMessages);
        logger.info("**Speed {}MPS",messageCount/durationInSecond);
        logger.info("**All task completed in {} seconds", durationInSecond);
    }
}

