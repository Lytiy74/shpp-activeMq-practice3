package shpp.azaika.util.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public final class Producer implements Callable<Integer>, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    private MessageProducer messageProducer;
    private Session session;
    private Connection connection;

    public static final String POISON_PILL = "POISON PILL 'DUDE STOP!'";

    private final ConnectionFactory connectionFactory;
    private final BlockingQueue<String> messageQueueSource;

    private final AtomicInteger messagesSent = new AtomicInteger(0);


    public Producer(ConnectionFactory connectionFactory, BlockingQueue<String> messageSourceQueue) {
        if (connectionFactory == null) {
            throw new IllegalArgumentException("ConnectionFactory must not be null");
        }
        if (messageSourceQueue == null) {
            throw new IllegalArgumentException("MessageQueueSource must not be null");
        }
        this.messageQueueSource = messageSourceQueue;
        this.connectionFactory = connectionFactory;
    }

    public void connect(String destinationName) throws JMSException {
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(destinationName);
            messageProducer = createMessageProducer(session, destination);
        } catch (JMSException e) {
            close();
            throw e;
        }
    }

    public void sendTextMessage(String text) throws JMSException {
        logger.info("Sending message: {}", text);
        TextMessage textMessage = session.createTextMessage(text);
        messageProducer.send(textMessage);
        messagesSent.incrementAndGet();
    }

    public void sendPoisonPill() throws JMSException {
        logger.info("Sending POISON PILL");
        sendTextMessage(POISON_PILL);
    }

    private MessageProducer createMessageProducer(Session session, Destination destination) throws JMSException {
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        return producer;
    }

    @Override
    public void close() {
        try {
            if (messageProducer != null) messageProducer.close();
            if (session != null) session.close();
            if (connection != null) connection.close();
        } catch (JMSException e) {
            logger.error("Error while closing JMS resources", e);
        }
    }

    @Override
    public Integer call() throws Exception {
        try {
            sendMessages();
        } finally {
            sendPoisonPill();
            close();
        }
        logger.info(Thread.currentThread().getName() + " has been finished.");
        return messagesSent.get();
    }

    private void sendMessages() {
        Stream.generate(this::pollMessageWithHandling)
                .takeWhile(Objects::nonNull)
                .takeWhile(msg -> !Thread.currentThread().isInterrupted())
                .forEach(this::processMessage);
    }

    private String pollMessageWithHandling() {
        try {
            return getStringFromQueue();
        } catch (InterruptedException e) {
            logger.info("Thread was interrupted during polling, exiting gracefully.");
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private void processMessage(String polledText) {
        try {
            sendTextMessage(polledText);
            if (messagesSent.get() % 1000 == 0) {
                logger.info("Sent {} messages", messagesSent.get());
            }
        } catch (JMSException e) {
            logger.error("Error sending message", e);
        }
    }


    private String getStringFromQueue() throws InterruptedException {
        String polledText = messageQueueSource.poll(1, TimeUnit.SECONDS);
        logger.debug("Polled message: {}", polledText);
        if (polledText == null) {
            logger.debug("Polling timed out.");
            return null;
        }
        return polledText;
    }

}