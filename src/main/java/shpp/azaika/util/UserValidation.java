package shpp.azaika.util;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.pojo.UserPojo;

import java.util.Set;

public class UserValidation {
    private static final Logger logger = LoggerFactory.getLogger(UserValidation.class);
    private final Validator validator;
    public UserValidation() {
        this.validator = Validation.buildDefaultValidatorFactory().getValidator();
    }

    public boolean isValid(UserPojo userPojo) {
        logger.info("Validating {}", userPojo);
        Set<ConstraintViolation<UserPojo>> violations = validator.validate(userPojo);

        return violations.isEmpty();
    }

    public Validator getValidator() {
        return validator;
    }
}
