package shpp.azaika.util.mq;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.jms.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

class ProducerTest {
    @Mock
    ConnectionFactory connectionFactoryMock;

    @Mock
    BlockingQueue<String> messageSourceQueueMock;

    @Mock
    Connection connectionMock;

    @Mock
    Session sessionMock;

    @Mock
    Queue destinationMock;

    @Mock
    MessageProducer producerMock;

    @Mock
    TextMessage textMessageMock;


    @BeforeEach
    void setUp() throws JMSException {
        MockitoAnnotations.openMocks(this);
        when(connectionFactoryMock.createConnection()).thenReturn(connectionMock);
        when(connectionMock.createSession(false, Session.AUTO_ACKNOWLEDGE)).thenReturn(sessionMock);
        when(sessionMock.createQueue("TestQueue")).thenReturn(destinationMock);
        when(sessionMock.createProducer(destinationMock)).thenReturn(producerMock);
        when(sessionMock.createTextMessage(anyString())).thenReturn(textMessageMock);

    }

    @Test
    void connect() throws JMSException {
        Producer producer = new Producer(connectionFactoryMock, messageSourceQueueMock);

        producer.connect("TestQueue");
        verify(connectionMock).start();
        verify(sessionMock).createQueue("TestQueue");
        verify(sessionMock).createProducer(destinationMock);
    }

    @Test
    void sendTextMessage() throws JMSException {
        Producer producer = new Producer(connectionFactoryMock, messageSourceQueueMock);
        producer.connect("TestQueue");
        TextMessage textMessageMock = mock(TextMessage.class);
        when(sessionMock.createTextMessage("Test message")).thenReturn(textMessageMock);

        producer.sendTextMessage("Test message");

        verify(sessionMock).createTextMessage("Test message");
        verify(producerMock).send(textMessageMock);
    }

    @Test
    void sendPoisonPill() throws JMSException {
        Producer producer = new Producer(connectionFactoryMock, messageSourceQueueMock);
        producer.connect("TestQueue");

        TextMessage poisonPillMessageMock = mock(TextMessage.class);
        when(sessionMock.createTextMessage(Producer.POISON_PILL)).thenReturn(poisonPillMessageMock);

        producer.sendPoisonPill();

        verify(sessionMock).createTextMessage(Producer.POISON_PILL);
        verify(producerMock).send(poisonPillMessageMock);
    }

    @Test
    void close() throws JMSException {
        Producer producer = new Producer(connectionFactoryMock, messageSourceQueueMock);
        producer.connect("TestQueue");

        producer.close();

        // Перевіряємо, чи були закриті всі ресурси
        verify(producerMock).close();
        verify(sessionMock).close();
        verify(connectionMock).close();
    }

    @Test
    void call() throws Exception {
        when(messageSourceQueueMock.poll(1, TimeUnit.SECONDS)).thenReturn("Message 1", "Message 2", null);

        Producer producer = new Producer(connectionFactoryMock, messageSourceQueueMock);
        producer.connect("TestQueue");

        producer.call();

        verify(producerMock, times(3)).send(any(TextMessage.class));
    }
}