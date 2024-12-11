package shpp.azaika;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.util.PropertyManager;
import shpp.azaika.util.UserPojoGenerator;
import shpp.azaika.util.mq.Consumer;
import shpp.azaika.util.mq.Producer;

import javax.jms.JMSException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws IOException {
        int n = Integer.parseInt(args[0]);

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        int sendedMessages = 0;

        PropertyManager propertyManager = new PropertyManager("app.properties");
        long timeForGenerationInSec = Integer.parseInt(propertyManager.getProperty("generation.duration"));
        String userName = propertyManager.getProperty("activemq.user");
        String userPassword = propertyManager.getProperty("activemq.pwd");
        String urlMq = propertyManager.getProperty("activemq.url");
        String destinationName = propertyManager.getProperty("activemq.queue");
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(userName, userPassword, urlMq);

        StopWatch stopWatch = new StopWatch(true);
        try (Producer producer = new Producer(connectionFactory)) {
            producer.connect(destinationName);
            for (int i = 0; i < n && stopWatch.taken() < timeForGenerationInSec; i++) {
                String userPojoJsonString = getUserPojoJsonString();
                producer.sendTextMessage(userPojoJsonString);
                sendedMessages++;
            }
            producer.sendPoisonPill();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }

        long wastedTimeInSeconds = TimeUnit.SECONDS.convert(stopWatch.stop(), TimeUnit.MILLISECONDS);
        double sendingMps = ((double) sendedMessages / wastedTimeInSeconds);
        logger.info("WASTED TIME {}s", wastedTimeInSeconds);
        logger.info("TOTAL SEND MESSAGES  {}", sendedMessages);
        logger.info("SENDING MESSAGE PER SECOND {}", sendingMps);

        try (Consumer consumer = new Consumer(connectionFactory, "valid.csv", "invalid.csv")) {
            consumer.connect(destinationName);
            consumer.start();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getUserPojoJsonString() throws JsonProcessingException {
        UserPojo userPojo = new UserPojoGenerator().generate();
        return new ObjectMapper().registerModule(new JavaTimeModule()).writeValueAsString(userPojo);
    }
}
