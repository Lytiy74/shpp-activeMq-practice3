package shpp.azaika.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.datafaker.Faker;
import net.datafaker.idnumbers.UkrainianIdNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.pojo.UserPojo;

import java.time.LocalDate;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

public class UserPojoGenerator {
    private static final Logger logger = LoggerFactory.getLogger(UserPojoGenerator.class);

    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    private final static Faker faker = new Faker(new Locale("uk_UA"));
    private UkrainianIdNumber ukrainianIdNumber;

    public UserPojoGenerator() {
        this.ukrainianIdNumber = new UkrainianIdNumber();
    }


    public UserPojo generate() {
        String name = generateName();
        String eddr = generateEddr();
        int count = generateCount();
        LocalDate date = LocalDate.now();
        return new UserPojo(name, eddr, count, date);
    }

    public String generateUserPojoAsJson() {
        UserPojo generate = generate();
        try {
            return mapper.writeValueAsString(generate);
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize UserPojo to JSON", e);
            return "{}";
        }
    }

    private int generateCount() {
        return ThreadLocalRandom.current().nextInt(0, 999);
    }

    private String generateEddr() {
        boolean valid = ThreadLocalRandom.current().nextBoolean();
        return valid ? ukrainianIdNumber.generateValid(faker) : ukrainianIdNumber.generateInvalid(faker);
    }

    private String generateName() {
        return faker.name().name();
    }
}
