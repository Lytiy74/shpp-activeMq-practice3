package shpp.azaika.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.pojo.UserPojo;
import shpp.azaika.util.mq.Producer;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.io.IOException;

public class MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);
    private final ObjectMapper mapper;
    private final UserValidation validator;
    private final Writer validWriter;
    private final Writer invalidWriter;

    public MessageHandler(ObjectMapper mapper, UserValidation validator, Writer writerForValid, Writer writerForInvalid) {
        this.mapper = mapper;
        this.validator = validator;
        this.validWriter = writerForValid;
        this.invalidWriter = writerForInvalid;
    }

    public void handleMessage(Message message) throws JMSException, IOException {
        if (message instanceof TextMessage) {
            handleTextMessage((TextMessage) message);
        } else {
            logger.warn("Received unsupported message type: {}", message.getClass().getSimpleName());
        }
    }

    public void handleTextMessage(TextMessage message) throws JMSException, IOException {
        String textFromMessage = message.getText();
        UserPojo userPojo = mapper.readValue(textFromMessage, UserPojo.class);
        boolean isValid = validator.isValid(userPojo);
        if (isValid) {
            logger.info("User is Valid, Write to Valid file");
            validWriter.write(userPojo);
        } else {
            logger.info("User is invalid, Write to invalid file");
            invalidWriter.write(userPojo);
        }
    }

    public boolean isPoisonPill(Message message) throws JMSException {
        TextMessage textMessage = (TextMessage) message;
        return textMessage.getText().equals(Producer.POISON_PILL);
    }
}
