package shpp.azaika.util.mq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import shpp.azaika.util.MessageHandler;

import javax.jms.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConsumerTest {

    @Mock
    private Message poisonMessageMock;
    @Mock
    private ActiveMQConnectionFactory connectionFactoryMock;
    @Mock
    private MessageHandler messageHandlerMock;
    @Mock
    private Connection connectionMock;
    @Mock
    private Session sessionMock;
    @Mock
    private MessageConsumer messageConsumerMock;

    private Consumer consumer;

    @BeforeEach
    void setUp() throws JMSException {
        MockitoAnnotations.openMocks(this);
        when(connectionFactoryMock.createConnection()).thenReturn(connectionMock);
        when(connectionMock.createSession(false, Session.AUTO_ACKNOWLEDGE)).thenReturn(sessionMock);
        when(sessionMock.createQueue("testQueue")).thenReturn(mock(Queue.class));
        when(sessionMock.createConsumer(any(Destination.class))).thenReturn(messageConsumerMock);

        consumer = new Consumer(connectionFactoryMock, messageHandlerMock);
    }

    @Test
    void constructorThrowsExceptionWhenArgumentsAreNull() {
        assertThrows(IllegalArgumentException.class, () -> new Consumer(null, messageHandlerMock));
        assertThrows(IllegalArgumentException.class, () -> new Consumer(connectionFactoryMock, null));
    }

    @Test
    void connectCreatesConnectionAndSession() throws Exception {
        consumer.connect("testQueue");

        verify(connectionFactoryMock).createConnection();
        verify(connectionMock).start();
        verify(sessionMock).createQueue("testQueue");
        verify(sessionMock).createConsumer(any(Destination.class));
    }

    @Test
    void connectThrowsExceptionWhenQueueNameIsInvalid() {
        assertThrows(IllegalArgumentException.class, () -> consumer.connect(null));
        assertThrows(IllegalArgumentException.class, () -> consumer.connect(""));
    }

    @Test
    void processNextMessageHandlesMessage() throws Exception {
        Message messageMock = mock(Message.class);

        when(messageConsumerMock.receive()).thenReturn(messageMock);
        when(messageHandlerMock.isPoisonPill(messageMock)).thenReturn(false);

        consumer.connect("testQueue");

        boolean result = consumer.processNextMessage();

        assertTrue(result);
        verify(messageHandlerMock).handleMessage(messageMock);
    }

    @Test
    void processNextMessageStopsOnPoisonPill() throws Exception {
        when(messageConsumerMock.receive()).thenReturn(poisonMessageMock);
        when(messageHandlerMock.isPoisonPill(poisonMessageMock)).thenReturn(true);

        consumer.connect("testQueue");

        boolean result = consumer.processNextMessage();

        assertFalse(result);
        verify(messageHandlerMock, never()).handleMessage(poisonMessageMock);
    }

    @Test
    void processNextMessageHandlesNullMessage() throws Exception {
        when(messageConsumerMock.receive()).thenReturn(null);

        consumer.connect("testQueue");

        boolean result = consumer.processNextMessage();

        assertFalse(result);
        verify(messageHandlerMock, never()).handleMessage(any());
    }

    @Test
    void closeReleasesResources() throws Exception {
        consumer.connect("testQueue");
        consumer.setConnection(connectionMock);
        consumer.setSession(sessionMock);
        consumer.setMessageConsumer(messageConsumerMock);

        consumer.close();

        verify(messageConsumerMock).close();
        verify(sessionMock).close();
        verify(connectionMock).close();
    }
}
