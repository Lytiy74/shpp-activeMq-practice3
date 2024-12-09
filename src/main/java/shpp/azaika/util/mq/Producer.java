package shpp.azaika.util.mq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.util.PropertyManager;

import javax.jms.*;

public final class Producer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    private final PropertyManager properties;
    private final MessageProducer messageProducer;
    private final Session session;
    private final Connection connection;
    public static final String POISON_PILL = "POISON PILL 'DUDE STOP!'";

    public Producer(PropertyManager properties) throws JMSException {
        this.properties = properties;
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(properties.getProperty("activemq.user"), properties.getProperty("activemq.pwd"), properties.getProperty("activemq.url"));
        connection = connectionFactory.createConnection();
        session = connection.createSession();
        messageProducer = getMessageProducer(session);
        logger.debug("Producer initialized");
    }

    public void sendTextMessage(String text) throws JMSException {
        logger.info("Sending message {}", text);
        TextMessage textMessage = session.createTextMessage(text);
        messageProducer.send(textMessage);
    }

    public void sendPoisonPill() throws JMSException {
        logger.info("Sending POISON PILL");
        TextMessage poisonPill = session.createTextMessage(POISON_PILL);
        messageProducer.send(poisonPill);
    }

    private MessageProducer getMessageProducer(Session session) throws JMSException {
        Destination destination = session.createQueue(properties.getProperty("activemq.queue"));
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        return producer;
    }


    @Override
    public void close() throws JMSException {
        connection.close();
        session.close();
        messageProducer.close();
    }
}
