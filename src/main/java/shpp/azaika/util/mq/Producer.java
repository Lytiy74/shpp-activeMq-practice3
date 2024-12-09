package shpp.azaika.util.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.UserPojo;
import shpp.azaika.util.PropertyManager;
import shpp.azaika.util.UserPojoGenerator;

import javax.jms.*;

public final class Producer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    private final PropertyManager properties;
    private final ActiveMQConnectionFactory connectionFactory;
    private final UserPojoGenerator userPojoGenerator;
    private final ObjectMapper objectMapper;
    private final int qtyOfMessages;
    private boolean running = true;
    private static final String POISON_PILL = "POISON PILL";

    public Producer(PropertyManager properties, UserPojoGenerator userPojoGenerator, int qtyOfMessages) {
        this.properties = properties;
        this.connectionFactory = new ActiveMQConnectionFactory(properties.getProperty("activemq.user"), properties.getProperty("activemq.pwd"), properties.getProperty("activemq.url"));
        this.userPojoGenerator = userPojoGenerator;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.qtyOfMessages = qtyOfMessages;
        logger.debug("Producer initialized");
    }

    @Override
    public void run() {
        try (Connection connection = connectionFactory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            connection.start();
            logger.info("Producer started");
            MessageProducer producer = getMessageProducer(session);
            for (int i = 0; i < qtyOfMessages && running; i++) {
                TextMessage textMessage = getTextMessage(session);
                producer.send(textMessage);
                logger.info("Message {} sent to {}", textMessage.getText(), producer.getDestination());
            }
        } catch (JMSException | JsonProcessingException e) {
            logger.error("Error producing message", e);
            throw new RuntimeException(e);
        }
        logger.info("Producer stopped");
    }

    public void stop(){
        this.running = false;
    }

    private MessageProducer getMessageProducer(Session session) throws JMSException {
        Destination destination = session.createQueue(properties.getProperty("activemq.queue"));
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        return producer;
    }

    private TextMessage getTextMessage(Session session) throws JsonProcessingException, JMSException {
        String userPojoString = getUserPojoString();
        TextMessage textMessage = session.createTextMessage(userPojoString);
        logger.debug("Message generated {}", userPojoString);
        return textMessage;
    }

    private String getUserPojoString() throws JsonProcessingException {
        UserPojo userPojo = userPojoGenerator.generate();
        String userPojoString = objectMapper.writeValueAsString(userPojo);
        return userPojoString;
    }

}
