package shpp.azaika.util.mq;


import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.util.MessageHandler;

import javax.jms.*;

public final class Consumer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final ActiveMQConnectionFactory connectionFactory;

    private Connection connection;
    private Session session;
    private MessageConsumer messageConsumer;
    private final MessageHandler messageHandler;

    public Consumer(ActiveMQConnectionFactory connectionFactory, MessageHandler messageHandler) {
        if (connectionFactory == null) {
            throw new IllegalArgumentException("ConnectionFactory must not be null");
        }
        this.messageHandler = messageHandler;
        this.connectionFactory = connectionFactory;
        logger.debug("Consumer initialized");
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
            if (message == null) return false;

            if (messageHandler.isPoisonPill(message)) {
                logger.info("Received poison pill, stopping processing.");
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
    public void close() throws JMSException {
        if (messageConsumer != null) messageConsumer.close();
        if (session != null) session.close();
        if (connection != null) connection.close();
    }
}
