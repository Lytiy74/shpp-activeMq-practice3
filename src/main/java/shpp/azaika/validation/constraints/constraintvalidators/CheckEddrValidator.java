package shpp.azaika.validation.constraints.constraintvalidators;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import shpp.azaika.validation.constraints.CheckEddr;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class CheckEddrValidator implements ConstraintValidator<CheckEddr,String> {
    @Override
    public void initialize(CheckEddr constraintAnnotation) {
        ConstraintValidator.super.initialize(constraintAnnotation);
    }

    @Override
    public boolean isValid(String eddr, ConstraintValidatorContext context) {
        return eddr != null && eddr.matches("\\d{8}-\\d{5}") && isDateValid(eddr);
    }

    private boolean isDateValid(String eddr) {
        try {
        LocalDate.parse(eddr.substring(0,8), DateTimeFormatter.BASIC_ISO_DATE);
        } catch (DateTimeParseException e){
            return false;
        }
        return true;
    }
}
