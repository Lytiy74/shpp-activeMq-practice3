package shpp.azaika;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.pojo.UserPojo;
import shpp.azaika.util.CsvWriter;
import shpp.azaika.util.MessageHandler;
import shpp.azaika.util.PropertyManager;
import shpp.azaika.util.UserPojoGenerator;
import shpp.azaika.util.mq.Consumer;
import shpp.azaika.util.mq.Producer;

import javax.jms.JMSException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws IOException {

        PropertyManager propertyManager = new PropertyManager("app.properties");
        String userName = propertyManager.getProperty("activemq.user");
        String userPassword = propertyManager.getProperty("activemq.pwd");
        String urlMq = propertyManager.getProperty("activemq.url");
        String destinationName = propertyManager.getProperty("activemq.queue");
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(userName, userPassword, urlMq);
        long timeForGenerationInSec = Integer.parseInt(propertyManager.getProperty("generation.duration"));

        int n = Integer.parseInt(args[0]);
        StopWatch stopWatch = new StopWatch(true);

        producer(n, connectionFactory, destinationName, stopWatch, timeForGenerationInSec);

        consumer(connectionFactory, destinationName, stopWatch);
    }

    private static void producer(int n, ActiveMQConnectionFactory connectionFactory, String destinationName, StopWatch stopWatch, long timeForGenerationInSec) throws JsonProcessingException {
        int sendedMessages = 0;
        try (Producer producer = new Producer(connectionFactory)) {
            producer.connect(destinationName);
            for (int i = 0; i < n && TimeUnit.SECONDS.convert(stopWatch.taken(),TimeUnit.MILLISECONDS) < timeForGenerationInSec; i++) {
                String userPojoJsonString = getUserPojoJsonString();
                producer.sendTextMessage(userPojoJsonString);
                sendedMessages++;
            }
            producer.sendPoisonPill();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }

        long spentTimeInSeconds = TimeUnit.SECONDS.convert(stopWatch.stop(), TimeUnit.MILLISECONDS);
        double sendingMps = ((double) sendedMessages / spentTimeInSeconds);
        logger.info("WASTED TIME {}s", spentTimeInSeconds);
        logger.info("TOTAL SEND MESSAGES  {}", sendedMessages);
        logger.info("SENDING MESSAGE PER SECOND {}", sendingMps);
    }

    private static void consumer(ActiveMQConnectionFactory connectionFactory, String destinationName, StopWatch stopWatch) throws FileNotFoundException {
        long wastedTimeInSeconds;
        int receivedMessages = 0;
        double receivingMps = 0;
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        MessageHandler messageHandler = new MessageHandler(mapper, validator, new CsvWriter("valid.csv"), new CsvWriter("invalid.csv"));
        try (Consumer consumer = new Consumer(connectionFactory, messageHandler)) {
            consumer.connect(destinationName);
            stopWatch.restart();
            while (true) {
                boolean processed = consumer.processNextMessage();
                if (!processed) break;
                receivedMessages++;
            }
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
        wastedTimeInSeconds = TimeUnit.SECONDS.convert(stopWatch.stop(), TimeUnit.MILLISECONDS);
        receivingMps = ((double)receivedMessages/wastedTimeInSeconds);
        logger.info("WASTED TIME {}s", wastedTimeInSeconds);
        logger.info("TOTAL RECEIVED MESSAGES  {}", receivedMessages);
        logger.info("RECEIVING MESSAGE PER SECOND {}", receivingMps);
    }

    private static String getUserPojoJsonString() throws JsonProcessingException {
        UserPojo userPojo = new UserPojoGenerator().generate();
        return new ObjectMapper().registerModule(new JavaTimeModule()).writeValueAsString(userPojo);
    }
}
