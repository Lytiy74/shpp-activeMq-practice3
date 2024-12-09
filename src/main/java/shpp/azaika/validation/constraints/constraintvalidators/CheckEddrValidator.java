package shpp.azaika.validation.constraints.constraintvalidators;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import shpp.azaika.validation.constraints.CheckEddr;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.stream.IntStream;

public class CheckEddrValidator implements ConstraintValidator<CheckEddr, String> {
    @Override
    public void initialize(CheckEddr constraintAnnotation) {
        ConstraintValidator.super.initialize(constraintAnnotation);
    }

    @Override
    public boolean isValid(String eddr, ConstraintValidatorContext context) {
        return eddr != null && eddr.matches("\\d{8}-\\d{5}") && isDateValid(eddr) && isValidControlNumber(eddr);
    }

    public boolean isDateValid(String eddr) {
        try {
            LocalDate date = LocalDate.parse(eddr.substring(0, 8), DateTimeFormatter.BASIC_ISO_DATE);
            int year = date.getYear();
            int currentYear = LocalDate.now().getYear();
            return year > 1900 && year < currentYear;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    //https://blog.uaid.net.ua/ua-id-passport-outside/#google_vignette
    public boolean isValidControlNumber(String eddr) {
        int[] weights = new int[]{7, 3, 1, 7, 3, 1, 7, 3, 1, 7, 3, 1};
        String eddrNumbers = eddr.replace("-", "");
        int sum = IntStream.range(0, eddrNumbers.length()-1)
                .map(i -> Character.getNumericValue(eddrNumbers.charAt(i)) * weights[i])
                .sum();
        int control = sum % 10;
        int providedControl = Character.getNumericValue(eddr.charAt(eddr.length() - 1));
        return control == providedControl;
    }
}
