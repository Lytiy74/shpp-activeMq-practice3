package shpp.azaika.util.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public final class Producer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    private MessageProducer messageProducer;
    private Session session;
    private Connection connection;

    public static final String POISON_PILL = "POISON PILL 'DUDE STOP!'";

    private final ConnectionFactory connectionFactory;

    public Producer(ConnectionFactory connectionFactory) {
        if (connectionFactory == null) {
            throw new IllegalArgumentException("ConnectionFactory must not be null");
        }
        this.connectionFactory = connectionFactory;
    }

    public void connect(String queueName) throws JMSException {
        if (queueName == null || queueName.isEmpty()) {
            throw new IllegalArgumentException("Queue name must not be null or empty");
        }

        try {
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            messageProducer = createMessageProducer(session, queueName);
        } catch (JMSException e) {
            close();
            throw e;
        }
    }

    public void sendTextMessage(String text) throws JMSException {
        if (text == null || text.isEmpty()) {
            throw new IllegalArgumentException("Message text must not be null or empty");
        }
        logger.info("Sending message: {}", text);
        TextMessage textMessage = session.createTextMessage(text);
        messageProducer.send(textMessage);
    }

    public void sendPoisonPill() throws JMSException {
        logger.info("Sending POISON PILL");
        sendTextMessage(POISON_PILL);
    }

    private MessageProducer createMessageProducer(Session session, String queueName) throws JMSException {
        Destination destination = session.createQueue(queueName);
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
}
