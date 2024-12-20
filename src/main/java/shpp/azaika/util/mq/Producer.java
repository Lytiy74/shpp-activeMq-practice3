package shpp.azaika.util.mq;

import org.apache.activemq.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.util.UserPojoGenerator;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class Producer implements Callable<Integer>, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    private final AtomicInteger messagesSent = new AtomicInteger(0);
    private final int messagesToSend;

    private final long durationInMillis;
    private StopWatch stopWatch;

    private final ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private MessageProducer messageProducer;

    public static final String POISON_PILL = "POISON PILL 'DUDE STOP!'";

    private final UserPojoGenerator pojoGenerator;

    public Producer(ConnectionFactory connectionFactory, UserPojoGenerator userPojoGenerator, int messagesToSend, long durationInMillis) {
        if (connectionFactory == null) {
            throw new IllegalArgumentException("ConnectionFactory must not be null");
        }
        if (userPojoGenerator == null) {
            throw new IllegalArgumentException("UserPojoGenerator must not be null");
        }
        if (messagesToSend < 0) {
            throw new IllegalArgumentException("Messages to send must be non-negative");
        }
        if (durationInMillis < 0){
            throw new IllegalArgumentException("Duration to execute must be non-negative");
        }
        this.connectionFactory = connectionFactory;
        this.pojoGenerator = userPojoGenerator;
        this.messagesToSend = messagesToSend;
        this.durationInMillis = durationInMillis;
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

    private MessageProducer createMessageProducer(Session session, Destination destination) throws JMSException {
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.setDisableMessageTimestamp(true);
        return producer;
    }

    public void sendTextMessage(String text) {
        try {
            logger.trace("Send message {}",text);
            TextMessage textMessage = session.createTextMessage(text);
            messageProducer.send(textMessage);
            messagesSent.getAndIncrement();
        } catch (JMSException e) {
            logger.error("Failed to send message: {}", text, e);
            throw new JMSRuntimeException(e.getMessage());
        }

    }

    public void sendPoisonPill() {
        logger.info("Sending POISON PILL");
        sendTextMessage(POISON_PILL);
    }

    public void sendPoisonPill(int consumersQty) {
        IntStream.range(0, consumersQty).forEach(i -> sendPoisonPill());
    }

    private void sendMessagesInBatch() {
        List<String> batch = new ArrayList<>();
        Stream.generate(pojoGenerator::generateUserPojoAsJson)
                .limit(messagesToSend)
                .takeWhile(o-> stopWatch.taken() < durationInMillis)
                .forEach(msg -> {
                    batch.add(msg);
                    if (batch.size() >= 1000) {
                        logger.info("Send batch of 1000 msg");
                        sendBatch(batch);
                        batch.clear();
                    }
                });

        if (!batch.isEmpty() && stopWatch.taken() < durationInMillis) {
            sendBatch(batch);
        }
    }

    private void sendBatch(List<String> batch) {
        batch.forEach(this::sendTextMessage);
    }

    @Override
    public Integer call() {
        logger.info("Producer thread started");
        stopWatch = new StopWatch(true);
        sendMessagesInBatch();
        logger.info("{} has been finished.", Thread.currentThread().getName());
        return messagesSent.get();
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
    public int getProducedMessageCount() {
        return messagesSent.get();
    }
}