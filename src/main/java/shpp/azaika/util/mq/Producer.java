package shpp.azaika.util.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.util.UserPojoGenerator;

import javax.jms.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public final class Producer implements Callable<Integer>, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    private MessageProducer messageProducer;
    private Session session;
    private Connection connection;

    public static final String POISON_PILL = "POISON PILL 'DUDE STOP!'";

    private final ConnectionFactory connectionFactory;

    private final AtomicInteger messagesSent = new AtomicInteger(0);

    private final UserPojoGenerator pojoGenerator;

    private final int messagesToSend;
    private final int consumers;

    public Producer(ConnectionFactory connectionFactory, UserPojoGenerator userPojoGenerator, int messagesToSend, int consumers) {
        if (connectionFactory == null) {
            throw new IllegalArgumentException("ConnectionFactory must not be null");
        }
        if (userPojoGenerator == null) {
            throw new IllegalArgumentException("UserPojoGenerator must not be null");
        }
        this.connectionFactory = connectionFactory;
        this.pojoGenerator = userPojoGenerator;
        this.messagesToSend = messagesToSend;
        this.consumers = consumers;
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
        logger.debug("Sending message: {}", text);
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
            logger.info("Producer thread started");
            sendMessages();
        } finally {
            for (int i = 0; i < consumers; i++) {
                sendPoisonPill();
            }
            close();
        }
        logger.info("{} has been finished.", Thread.currentThread().getName());
        return messagesSent.get();
    }

    private void sendMessages() {
        Stream.generate(pojoGenerator::generateUserPojoAsJson)
                .takeWhile(o -> messagesSent.get() < messagesToSend)
                .takeWhile(msg -> !Thread.currentThread().isInterrupted())
                .forEach(this::processMessage);
    }

    private void processMessage(String polledText) {
        try {
            sendTextMessage(polledText);
        } catch (JMSException e) {
            logger.error("Error sending message", e);
        }
    }


}