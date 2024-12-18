package shpp.azaika.util.managers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.activemq.ActiveMQConnectionFactory;
import shpp.azaika.pojo.UserPojo;
import shpp.azaika.util.MessageHandler;
import shpp.azaika.util.mq.Consumer;

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ConsumerManager {
    private final List<Consumer> consumers = new ArrayList<>();
    private final ExecutorService consumerExecutor;
    private final BlockingQueue<UserPojo> validQueue;
    private final BlockingQueue<UserPojo> invalidQueue;

    public ConsumerManager(int consumerQty) {
        consumerExecutor = Executors.newFixedThreadPool(consumerQty);
        validQueue = new LinkedBlockingDeque<>(consumerQty * 3000);
        invalidQueue = new LinkedBlockingDeque<>(consumerQty * 3000);
    }

    public void startConsumers(ActiveMQConnectionFactory connectionFactory, String destinationName, int consumerQty) throws JMSException {
        for (int i = 0; i < consumerQty; i++) {
             ObjectMapper objectMapper = new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
            Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
            Consumer consumer = new Consumer(connectionFactory, new MessageHandler(objectMapper, validator, validQueue, invalidQueue));
            consumers.add(consumer);
            consumer.connect(destinationName);
            consumerExecutor.submit(consumer);
        }
    }

    public ExecutorService getExecutor() {
        return consumerExecutor;
    }

    public void shutdownConsumers() {
        consumers.forEach(Consumer::close);
    }

    public BlockingQueue<UserPojo> getValidQueue() {
        return validQueue;
    }

    public BlockingQueue<UserPojo> getInvalidQueue() {
        return invalidQueue;
    }

    public int getConsumedMessageCount() {
        return consumers.stream().mapToInt(Consumer::getConsumedMessagesCount).sum();
    }
}

