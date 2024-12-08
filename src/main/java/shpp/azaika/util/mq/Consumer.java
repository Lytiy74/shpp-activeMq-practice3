package shpp.azaika.util.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.UserPojo;
import shpp.azaika.util.PropertyManager;

import javax.jms.*;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Set;

public final class Consumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final PropertyManager properties;
    private final ActiveMQConnectionFactory connectionFactory;
    private final ObjectMapper objectMapper;
    private final CsvMapper csvMapper;

    public Consumer(PropertyManager properties) {
        this.properties = properties;
        this.connectionFactory = new ActiveMQConnectionFactory(properties.getProperty("activemq.user"), properties.getProperty("activemq.pwd"), properties.getProperty("activemq.url"));
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.csvMapper = new CsvMapper();
        this.csvMapper.registerModule(new JavaTimeModule());
        logger.debug("Consumer initialized");
    }

    @Override
    public void run() {
        try (Connection connection = connectionFactory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            logger.debug("Consumer started");
            connection.start();
            MessageConsumer consumer = session.createConsumer(session.createQueue(properties.getProperty("activemq.queue")));
            while (true) {
                Message receive = consumer.receive(5000);
                if (receive != null) {
                    handleMessage(receive);
                    continue;
                }
                break;
            }
        } catch (JMSException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void handleMessage(Message receive) throws JMSException, IOException {
        TextMessage textMessage = (TextMessage) receive;
        logger.debug("Message received: {}", textMessage.getText());
        UserPojo userPojo = objectMapper.readValue(textMessage.getText(), UserPojo.class);
        validateUserPojo(userPojo);
    }

    private void validateUserPojo(UserPojo userPojo) throws IOException {
        logger.debug("Validating user pojo: {}", userPojo);
        try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
            Validator validator = factory.getValidator();
            Set<ConstraintViolation<UserPojo>> violations = validator.validate(userPojo);
            if (!violations.isEmpty()) {
                logger.error("UserPojo {} validation failed: {}", userPojo, violations);
                writeToFile("invalid.csv", userPojo);
            } else {
                logger.debug("UserPojo {} validated", userPojo);
                writeToFile("valid.csv", userPojo);
            }
        }
    }

    private void writeToFile(String fileName, UserPojo userPojo) throws IOException {
        logger.debug("Writing to {}", fileName);
        CsvSchema schema = csvMapper.schemaFor(UserPojo.class);
        FileOutputStream out = new FileOutputStream(fileName, true);
        csvMapper.writer(schema).writeValue(out, userPojo);
        out.close();
        logger.debug("Written to {}", fileName);
    }


}
