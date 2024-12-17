package shpp.azaika.util.mq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.util.MessageHandler;

import javax.jms.*;
import java.util.concurrent.Callable;

public final class Consumer implements Callable<Integer>, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final ActiveMQConnectionFactory connectionFactory;

    private Connection connection;
    private Session session;
    private MessageConsumer messageConsumer;
    private final MessageHandler messageHandler;

    private volatile boolean running = true;

    public Consumer(ActiveMQConnectionFactory connectionFactory, MessageHandler messageHandler) {
        if (connectionFactory == null) {
            throw new IllegalArgumentException("ConnectionFactory must not be null");
        }
        if (messageHandler == null) {
            throw new IllegalArgumentException("MessageHandler must not be null");
        }
        this.messageHandler = messageHandler;
        this.connectionFactory = connectionFactory;
    }

    public void connect(String destinationName) throws JMSException {
        if (destinationName == null || destinationName.isEmpty()) {
            throw new IllegalArgumentException("Queue name must not be null or empty");
        }
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            messageConsumer = createMessageConsumer(session, destinationName);
            logger.debug("Connected to queue: {}", destinationName);
        } catch (JMSException e) {
            close();
            throw e;
        }
    }

    private MessageConsumer createMessageConsumer(Session session, String destinationName) throws JMSException {
        Destination destination = session.createQueue(destinationName);
        return session.createConsumer(destination);
    }

    public boolean processNextMessage() {
        try {
            Message message = messageConsumer.receive();
            if (message == null) {
                logger.warn("Received null message, stopping consumer.");
                return false;
            }

            if (messageHandler.isPoisonPill(message)) {
                logger.info("Received poison pill, stopping consumer.");
                running = false;
                return false;
            }

            messageHandler.handleMessage(message);
            return true;
        } catch (Exception e) {
            logger.error("Error processing message", e);
            return false;
        }
    }

    @Override
    public Integer call() {
        int processedMessages = 0;
        try {
            logger.info("Consumer thread started");
            while (running) {
                if (!processNextMessage()) {
                    break;
                }
                processedMessages++;
                if (processedMessages % 1000 == 0) {
                    logger.debug("Processed messages count: {}", processedMessages);
                }
            }
        } catch (Exception e) {
            logger.error("Unexpected error in consumer thread", e);
        } finally {
                close();
            logger.info("Consumer thread finished. Total processed messages: {}", processedMessages);
        }
        return processedMessages;
    }

    @Override
    public void close() {
        try {
            if (messageConsumer != null) messageConsumer.close();
            if (session != null) session.close();
            if (connection != null) connection.close();
        } catch (JMSException e) {
            logger.error("Error while closing JMS resources", e);
        }
    }
}
