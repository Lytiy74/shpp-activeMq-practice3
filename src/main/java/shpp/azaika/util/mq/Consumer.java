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
import java.io.*;
import java.util.Set;

public final class Consumer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final ObjectMapper objectMapper;
    private final ActiveMQConnectionFactory connectionFactory;
    private final Validator validator;
    private final CsvMapper csvMapper;
    private Connection connection;
    private Session session;
    private MessageConsumer messageConsumer;
    private boolean isRunning;

    private final String validFileName;
    private final String invalidFileName;

    private final BufferedOutputStream outputStreamForValid;
    private final BufferedOutputStream outputStreamForInvalid;

    public Consumer(ActiveMQConnectionFactory connectionFactory, String validFileName, String invalidFileName) throws FileNotFoundException {
        if (connectionFactory == null) {
            throw new IllegalArgumentException("ConnectionFactory must not be null");
        }
        this.connectionFactory = connectionFactory;
        this.validator = Validation.buildDefaultValidatorFactory().getValidator();
        this.isRunning = true;
        this.validFileName = validFileName;
        this.invalidFileName = invalidFileName;
        this.csvMapper = (CsvMapper) new CsvMapper().registerModule(new JavaTimeModule());
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        this.outputStreamForInvalid = new BufferedOutputStream(new FileOutputStream(validFileName, true));
        this.outputStreamForValid = new BufferedOutputStream(new FileOutputStream(invalidFileName, true));

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
                logger.info("Write {} to file {}",userPojo, validFileName);
                writeToFile(outputStreamForValid, userPojo);
            } else {
                logger.info("Write {} to file {}",userPojo, invalidFileName);
                writeToFile(outputStreamForInvalid, userPojo);
            }
        } catch (JsonProcessingException e) {
            logger.error("Failed to parse JSON message: {}", text, e);
        }
    }

    private void writeToFile(OutputStream outputStream, UserPojo userPojo) throws IOException {
        CsvSchema schema = csvMapper.schemaFor(UserPojo.class);
        csvMapper.writer(schema).writeValue(outputStream, userPojo);
    }

    private UserPojo mapTextToUserPojo(String text) throws JsonProcessingException {
        return objectMapper.readValue(text, UserPojo.class);
    }

    @Override
    public void close() throws JMSException {
        if (messageConsumer != null) messageConsumer.close();
        if (session != null) session.close();
        if (connection != null) connection.close();
    }
}
