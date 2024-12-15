package shpp.azaika.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.pojo.UserPojo;
import shpp.azaika.util.mq.Producer;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);

    private final ObjectMapper mapper;
    private final Validator validator;
    private final BlockingQueue<UserPojo> validQueue;
    private final BlockingQueue<UserPojo> invalidQueue;

    public MessageHandler(ObjectMapper mapper, Validator validator, BlockingQueue<UserPojo> validQueue, BlockingQueue<UserPojo> invalidQueue) {
        if (mapper == null || validator == null || validQueue == null || invalidQueue == null) {
            throw new IllegalArgumentException("Constructor arguments must not be null");
        }
        this.mapper = mapper;
        this.validator = validator;
        this.validQueue = validQueue;
        this.invalidQueue = invalidQueue;
    }

    public void handleMessage(Message message) throws JMSException, IOException, InterruptedException {
        if (message instanceof TextMessage textMessage) {
            handleTextMessage(textMessage);
        } else {
            logger.warn("Received unsupported message type: {}", message.getClass().getSimpleName());
        }
    }

    private void handleTextMessage(TextMessage message) throws JMSException, IOException, InterruptedException {
        String textFromMessage = message.getText();
        try {
            UserPojo userPojo = mapper.readValue(textFromMessage, UserPojo.class);
            validateAndRouteMessage(userPojo);
        } catch (IOException e) {
            logger.error("Failed to deserialize message content: {}", textFromMessage, e);
        }
    }

    private void validateAndRouteMessage(UserPojo userPojo) throws InterruptedException {
        Set<ConstraintViolation<UserPojo>> violations = validator.validate(userPojo);
        if (violations.isEmpty()) {
            validQueue.put(userPojo);
            logger.debug("Valid message routed to validQueue: {}", userPojo);
        } else {
            invalidQueue.put(userPojo);
            logValidationErrors(userPojo, violations);
        }
    }

    private void logValidationErrors(UserPojo userPojo, Set<ConstraintViolation<UserPojo>> violations) {
        logger.debug("Validation failed for UserPojo: {}", userPojo);
        for (ConstraintViolation<UserPojo> violation : violations) {
            logger.debug("Property '{}' {} (invalid value: {})",
                    violation.getPropertyPath(),
                    violation.getMessage(),
                    violation.getInvalidValue());
        }
    }

    public boolean isPoisonPill(Message message) throws JMSException {
        if (message instanceof TextMessage textMessage) {
            String text = textMessage.getText();
            return Producer.POISON_PILL.equals(text);
        }
        logger.warn("Received non-TextMessage for poison pill check: {}", message.getClass().getSimpleName());
        return false;
    }
}
