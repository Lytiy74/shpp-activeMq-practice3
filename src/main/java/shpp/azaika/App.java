package shpp.azaika;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.util.PropertyManager;
import shpp.azaika.util.UserPojoGenerator;
import shpp.azaika.util.mq.Consumer;
import shpp.azaika.util.mq.Producer;

import javax.jms.JMSException;
import java.io.IOException;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws IOException {
        PropertyManager propertyManager = new PropertyManager("app.properties");

        int n = Integer.parseInt(args[0]);

        int timeForGenerationInSec = Integer.parseInt(propertyManager.getProperty("generation.duration"));

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        int sendedMessages = 0;

        long startAt = System.currentTimeMillis();
        try (Producer producer = new Producer(propertyManager)) {
            for (int i = 0; i < n; i++) {
                if (System.currentTimeMillis() - startAt <= timeForGenerationInSec) {
                    String userPojoJsonString = getUserPojoJsonString();
                    producer.sendTextMessage(userPojoJsonString);
                    sendedMessages++;
                    continue;
                }
                producer.sendPoisonPill();
                break;
            }
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
        double wastedTimeInSeconds = (double) (System.currentTimeMillis() - startAt) / 1000;
        double sendingMps = (sendedMessages / wastedTimeInSeconds);
        logger.info("WASTED TIME {}s",wastedTimeInSeconds);
        logger.info("TOTAL SEND MESSAGES  {}", sendedMessages);
        logger.info("SENDING MESSAGE PER SECOND {}", sendingMps);

        try(Consumer consumer = new Consumer(propertyManager)){
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
