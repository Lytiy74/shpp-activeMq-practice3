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

    private final AtomicInteger messagesSent = new AtomicInteger(0);
    private final AtomicInteger messagesToSend;

    private final ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private MessageProducer messageProducer;

    public static final String POISON_PILL = "POISON PILL 'DUDE STOP!'";

    private final UserPojoGenerator pojoGenerator;

    public Producer(ConnectionFactory connectionFactory, UserPojoGenerator userPojoGenerator, int messagesToSend) {
        if (connectionFactory == null) {
            throw new IllegalArgumentException("ConnectionFactory must not be null");
        }
        if (userPojoGenerator == null) {
            throw new IllegalArgumentException("UserPojoGenerator must not be null");
        }
        this.connectionFactory = connectionFactory;
        this.pojoGenerator = userPojoGenerator;
        this.messagesToSend = new AtomicInteger(messagesToSend);
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
        return producer;
    }

    public void sendTextMessage(String text) {
        try {
            logger.debug("Sending message #{}: {}", messagesSent.getAndIncrement(), text);
            TextMessage textMessage = session.createTextMessage(text);
            messageProducer.send(textMessage);
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage());
        }

    }

    public void sendPoisonPill() {
        logger.info("Sending POISON PILL");
        sendTextMessage(POISON_PILL);
    }

    public void sendPoisonPill(int consumersQty) {
        for (int i = 0; i < consumersQty; i++) {
            sendPoisonPill();
        }
    }

    private void sendMessages() {
        Stream.generate(pojoGenerator::generateUserPojoAsJson)
                .takeWhile(o -> 0 < messagesToSend.getAndDecrement())
                .takeWhile(msg -> !Thread.currentThread().isInterrupted())
                .forEach(this::sendTextMessage);
    }

    @Override
    public Integer call() throws Exception {
        logger.info("Producer thread started");
        sendMessages();
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

}