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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Set;

public final class Consumer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final PropertyManager properties;
    private final Connection connection;
    private final Session session;
    private final MessageConsumer messageConsumer;
    private boolean run;

    public Consumer(PropertyManager properties) throws JMSException {
        this.properties = properties;
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(properties.getProperty("activemq.user"), properties.getProperty("activemq.pwd"), properties.getProperty("activemq.url"));
        connection = connectionFactory.createConnection();
        session = connection.createSession();
        messageConsumer = getMessageConsumer(session);
        run = true;
        logger.debug("Consumer initialized");
    }


    private MessageConsumer getMessageConsumer(Session session) throws JMSException {
        Destination destination = session.createQueue(properties.getProperty("activemq.queue"));
        return session.createConsumer(destination);
    }

    public void start() throws JMSException, IOException {
        connection.start();
        while (run){
            Message receive = messageConsumer.receive();
            if (receive != null) messageHandler(receive);
        }
    }

    private void messageHandler(Message message) throws JMSException, IOException {
        if (message instanceof TextMessage){
            String text = ((TextMessage) message).getText();
            if (text.equals(Producer.POISON_PILL)) {
                run = false;
                return;
            }
            validateMessage(text);
            return;
        }
        throw new IllegalArgumentException();
    }

    private void validateMessage(String text) throws IOException {
        UserPojo userPojo = mapTextToUserPojo(text);

        Set<ConstraintViolation<UserPojo>> violations = getViolation(userPojo);
        if (violations.isEmpty()){
           writeToFile("valid.csv",userPojo);
        }else {
            writeToFile("invalid.csv",userPojo);
        }
    }

    private void writeToFile(String fileName, UserPojo userPojo) throws IOException {
        logger.debug("Writing to {}", fileName);
        CsvMapper csvMapper = new CsvMapper();
        csvMapper.registerModule(new JavaTimeModule());
        CsvSchema schema = csvMapper.schemaFor(UserPojo.class);
        FileOutputStream out = new FileOutputStream(fileName, true);
        csvMapper.writer(schema).writeValue(out, userPojo);
        out.close();
        logger.debug("Written to {}", fileName);

    }

    private static Set<ConstraintViolation<UserPojo>> getViolation(UserPojo userPojo) {
        ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory();
        Validator validator = validatorFactory.getValidator();
        Set<ConstraintViolation<UserPojo>> validate = validator.validate(userPojo);
        return validate;
    }

    private static UserPojo mapTextToUserPojo(String text) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        UserPojo userPojo = objectMapper.readValue(text, UserPojo.class);
        return userPojo;
    }

    @Override
    public void close() throws JMSException {
        connection.close();
        session.close();
        messageConsumer.close();
    }
}
