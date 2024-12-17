package shpp.azaika;

import org.apache.activemq.ActiveMQConnectionFactory;
import shpp.azaika.util.UserPojoGenerator;
import shpp.azaika.util.mq.Producer;

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProducerManager {
    private final List<Producer> producers = new ArrayList<>();
    private final ExecutorService producerExecutor;
    private final int consumersQty;

    public ProducerManager(int producerQty, int consumersQty) throws JMSException {
        producerExecutor = Executors.newFixedThreadPool(producerQty);
        this.consumersQty = consumersQty;
    }

    public void startProducers(ActiveMQConnectionFactory connectionFactory, String destinationName, int producerQty, int messagesToSend) throws JMSException {
        int messagesPerThread = messagesToSend / producerQty;
        int pendingMessages = messagesToSend % producerQty;

        for (int i = 0; i < producerQty; i++) {
            int messagesForThisThread = (i == producerQty - 1) ? messagesPerThread + pendingMessages : messagesPerThread;
            Producer producer = new Producer(connectionFactory, new UserPojoGenerator(), messagesForThisThread);
            producers.add(producer);
            producer.connect(destinationName);
            producerExecutor.submit(producer);
        }
    }

    public ExecutorService getExecutor() {
        return producerExecutor;
    }

    public void shutdownProducers() {
        producers.getFirst().sendPoisonPill(consumersQty);
        producers.forEach(Producer::close);
    }

    public int getProducedMessageCount() {
        return producers.stream().mapToInt(Producer::getProducedMessageCount).sum();
    }
}
