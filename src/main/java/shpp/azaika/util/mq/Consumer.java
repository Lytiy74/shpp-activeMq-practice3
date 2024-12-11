package shpp.azaika.util.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.UserPojo;

import javax.jms.*;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Set;

public final class Consumer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final ActiveMQConnectionFactory connectionFactory;
    private final Validator validator;
    private Connection connection;
    private Session session;
    private MessageConsumer messageConsumer;
    private boolean isRunning;

    private final String validFileName;
    private final String invalidFileName;

    public Consumer(ActiveMQConnectionFactory connectionFactory, String validFileName, String invalidFileName) {
        if (connectionFactory == null) {
            throw new IllegalArgumentException("ConnectionFactory must not be null");
        }
        this.connectionFactory = connectionFactory;
        this.validator = Validation.buildDefaultValidatorFactory().getValidator();
        this.isRunning = true;
        this.validFileName = validFileName;
        this.invalidFileName = invalidFileName;
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

    public void start() throws JMSException {
        connection.start();
        try {
            while (isRunning) {
                Message receive = messageConsumer.receive();
                if (receive != null) messageHandler(receive);
            }
        } catch (Exception e) {
            logger.error("Error while consuming messages", e);
        }
    }

    private void messageHandler(Message message) throws JMSException, IOException {
        if (message instanceof TextMessage) {
            String text = ((TextMessage) message).getText();
            if (text.equals(Producer.POISON_PILL)) {
                isRunning = false;
                return;
            }
            validateMessage(text);
        } else {
            logger.warn("Received unsupported message type: {}", message.getClass().getSimpleName());
        }
    }

    private void validateMessage(String text) throws IOException {
        try {
            UserPojo userPojo = mapTextToUserPojo(text);
            Set<ConstraintViolation<UserPojo>> violations = validator.validate(userPojo);

            if (violations.isEmpty()) {
                writeToFile(validFileName, userPojo);
            } else {
                writeToFile(invalidFileName, userPojo);
            }
        } catch (JsonProcessingException e) {
            logger.error("Failed to parse JSON message: {}", text, e);
        }
    }

    private void writeToFile(String fileName, UserPojo userPojo) throws IOException {
        logger.debug("Writing to {}", fileName);
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(fileName, true))) {
            CsvMapper csvMapper = new CsvMapper();
            csvMapper.registerModule(new JavaTimeModule());
            CsvSchema schema = csvMapper.schemaFor(UserPojo.class);
            csvMapper.writer(schema).writeValue(out, userPojo);
        }
        logger.debug("Written to {}", fileName);
    }

    private static UserPojo mapTextToUserPojo(String text) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        return objectMapper.readValue(text, UserPojo.class);
    }

    @Override
    public void close() throws JMSException {
        if (messageConsumer != null) messageConsumer.close();
        if (session != null) session.close();
        if (connection != null) connection.close();
    }
}
