package shpp.azaika.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import shpp.azaika.pojo.UserPojo;
import shpp.azaika.util.mq.Producer;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MessageHandlerTest {

    @Mock
    private ObjectMapper mapper;
    @Mock
    private Validator validator;
    @Mock
    private TextMessage textMessage;

    private BlockingQueue<UserPojo> validQueue;
    private BlockingQueue<UserPojo> invalidQueue;

    private MessageHandler messageHandler;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        validQueue = new ArrayBlockingQueue<>(10);
        invalidQueue = new ArrayBlockingQueue<>(10);
        messageHandler = new MessageHandler(mapper, validator, validQueue, invalidQueue);
    }

    @Test
    void testConstructorWithNullArguments() {
        assertThrows(IllegalArgumentException.class, () -> new MessageHandler(null, validator, validQueue, invalidQueue));
        assertThrows(IllegalArgumentException.class, () -> new MessageHandler(mapper, null, validQueue, invalidQueue));
        assertThrows(IllegalArgumentException.class, () -> new MessageHandler(mapper, validator, null, invalidQueue));
        assertThrows(IllegalArgumentException.class, () -> new MessageHandler(mapper, validator, validQueue, null));
    }

    @Test
    void testHandleValidMessage() throws Exception {
        UserPojo userPojo = new UserPojo();
        String messageContent = "{\"name\":\"Андрій\"}";

        when(textMessage.getText()).thenReturn(messageContent);
        when(mapper.readValue(messageContent, UserPojo.class)).thenReturn(userPojo);
        when(validator.validate(userPojo)).thenReturn(Collections.emptySet());

        messageHandler.handleMessage(textMessage);

        assertTrue(validQueue.contains(userPojo));
        assertTrue(invalidQueue.isEmpty());
    }

    @Test
    void testHandleInvalidMessage() throws Exception {
        UserPojo userPojo = new UserPojo();
        String messageContent = "{\"name\":\"Федоришин Буревіст Сергійович\",\"eddr\":\"19830605-26112\",\"count\":814,\"date\":\"2024-12-18\"}";

        when(textMessage.getText()).thenReturn(messageContent);
        when(mapper.readValue(messageContent, UserPojo.class)).thenReturn(userPojo);
        when(validator.validate(userPojo)).thenReturn(Set.of(mock(ConstraintViolation.class)));

        messageHandler.handleMessage(textMessage);

        assertTrue(invalidQueue.contains(userPojo));
        assertTrue(validQueue.isEmpty());
    }

    @Test
    void testPoisonPillDetection() throws JMSException {
        when(textMessage.getText()).thenReturn(Producer.POISON_PILL);

        boolean isPoisonPill = messageHandler.isPoisonPill(textMessage);

        assertTrue(isPoisonPill);
    }

    @Test
    void testHandleMessageWithInvalidJson() throws Exception {
        String invalidJson = "invalid_json";

        when(textMessage.getText()).thenReturn(invalidJson);
        when(mapper.readValue(invalidJson, UserPojo.class)).thenThrow(JsonProcessingException.class);

        messageHandler.handleMessage(textMessage);

        assertTrue(validQueue.isEmpty());
        assertTrue(invalidQueue.isEmpty());
    }

}
