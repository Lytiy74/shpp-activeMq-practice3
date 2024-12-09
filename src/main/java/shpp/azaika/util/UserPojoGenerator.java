package shpp.azaika.util;

import net.datafaker.Faker;
import net.datafaker.idnumbers.UkrainianIdNumber;
import shpp.azaika.UserPojo;

import java.time.LocalDate;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

public class UserPojoGenerator {
    private final Faker faker = new Faker(new Locale("uk_UA"));
    public UserPojo generate() {
        String name = generateName();
        String eddr = generateEddr();
        int count = generateCount();
        LocalDate date = LocalDate.now();
        return new UserPojo(name, eddr, count, date);
    }

    private int generateCount() {
        return ThreadLocalRandom.current().nextInt(0, 999);
    }

    private String generateEddr() {
        UkrainianIdNumber ukrainianIdNumber = new UkrainianIdNumber();
        boolean valid = ThreadLocalRandom.current().nextBoolean();
        return valid ? ukrainianIdNumber.generateValid(faker) : ukrainianIdNumber.generateInvalid(faker);
    }

    private String generateName() {
        return faker.name().fullName();
    }
}
