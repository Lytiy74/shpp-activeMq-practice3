package shpp.azaika;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import shpp.azaika.validation.constraints.constraintvalidators.CheckEddrValidator;

import java.time.LocalDate;
import java.util.Locale;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UserPojoValidationTest {
   private static Validator validator;
   private static CheckEddrValidator eddrValidator;
    @BeforeAll
    static void init(){
        Locale.setDefault(Locale.ENGLISH);
        ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory();
        validator = validatorFactory.getValidator();
        eddrValidator = new CheckEddrValidator();
    }


    @Nested
    class CheckEddrValidatorMethodsTests {
        @Test
        void testValidationOfValidEddr() {
            String[] validEddr = new String[]{"19760506-26583", "19760329-99102", "19910428-67856", "19670727-73375", "19751006-23893"};
            for (String s : validEddr) assertTrue(eddrValidator.isValid(s, null));
        }

        @Test
        void testValidationOfInvalidEddr() {
            String[] validEddr = new String[]{"19690304-90994", "20050407-18286", "20040731-20888", "19830213-67306", "19810426-17833"};
            for (String s : validEddr) assertFalse(eddrValidator.isValid(s, null));
        }

        @Test
        void testValidationInvalidFormat() {
            String eddr = "19760506-2650583";
            assertFalse(eddrValidator.isValid(eddr, null));
        }

        @Test
        void testValidationControlNumberValid() {
            String eddr = "19760506-26583";
            assertTrue(eddrValidator.isValidControlNumber(eddr));
        }

        @Test
        void testValidationControlNumberInvalid() {
            String eddr = "19760506-26580";
            assertFalse(eddrValidator.isValidControlNumber(eddr));
        }

        @Test
        void testValidationDateFormatValid() {
            String eddr = "19760506-26583";
            assertTrue(eddrValidator.isDateValid(eddr));
        }

        @Test
        void testValidationDateFormatInvalid() {
            String eddr = "05200412-26583";
            assertFalse(eddrValidator.isDateValid(eddr));
        }
    }

    @Nested
    class HibernateValidatorTests{
        @Test
        void testValidNameEN(){
            UserPojo userPojo = new UserPojo("Andrew Zaika","19760506-26583",10, LocalDate.now());
            Set<ConstraintViolation<UserPojo>> validate = validator.validate(userPojo);
            assertTrue(validate.isEmpty());
        }

        @Test
        void testValidNameUKR(){
            UserPojo userPojo = new UserPojo("Андрій Заїка","19760506-26583",10, LocalDate.now());
            Set<ConstraintViolation<UserPojo>> validate = validator.validate(userPojo);
            validate.forEach(System.out::println);
            assertTrue(validate.isEmpty());
        }
        @Test
        void testInvalidNameLength(){
            UserPojo userPojo = new UserPojo("Andrew","19760506-26583",10, LocalDate.now());
            Set<ConstraintViolation<UserPojo>> validate = validator.validate(userPojo);
            boolean hasExpectedMessage = validate.stream()
                    .anyMatch(violation -> "length must be between 7 and 2147483647".equals(violation.getMessage()));

            assertTrue(hasExpectedMessage, "Expected validation message not found.");
        }

        @Test
        void testInvalidNameContainsLetterA(){
            UserPojo userPojo = new UserPojo("Коля Колян","19760506-26583",10, LocalDate.now());
            Set<ConstraintViolation<UserPojo>> validate = validator.validate(userPojo);
            boolean hasExpectedMessage = validate.stream()
                    .anyMatch(violation -> "Name must contains at least one letter 'A'".equals(violation.getMessage()));

            assertTrue(hasExpectedMessage, "Expected validation message not found.");
        }

        @Test
        void testInvalidEddr(){
            UserPojo userPojo = new UserPojo("Andrew Zaika","19760506-26585",10, LocalDate.now());
            Set<ConstraintViolation<UserPojo>> validate = validator.validate(userPojo);
            boolean hasExpectedMessage = validate.stream()
                    .anyMatch(violation -> "Invalid eddr number".equals(violation.getMessage()));

            assertTrue(hasExpectedMessage, "Expected validation message not found.");
        }

        @Test
        void testValidEddr(){
            UserPojo userPojo = new UserPojo("Andrew Zaika","19760506-26583",10, LocalDate.now());
            Set<ConstraintViolation<UserPojo>> validate = validator.validate(userPojo);
            boolean hasExpectedMessage = validate.stream()
                    .anyMatch(violation -> "Invalid eddr number".equals(violation.getMessage()));

            assertFalse(hasExpectedMessage);
        }

        @Test
        void testInvalidCount(){
            UserPojo userPojo = new UserPojo("Andrew Zaika","19760506-26583",5, LocalDate.now());
            Set<ConstraintViolation<UserPojo>> validate = validator.validate(userPojo);
            boolean hasExpectedMessage = validate.stream()
                    .anyMatch(violation -> "Count must be >= 10".equals(violation.getMessage()));

            assertTrue(hasExpectedMessage, "Expected validation message not found.");
        }

        @Test
        void testValidCount(){
            UserPojo userPojo = new UserPojo("Andrew Zaika","19760506-26583",10, LocalDate.now());
            Set<ConstraintViolation<UserPojo>> validate = validator.validate(userPojo);
            boolean hasExpectedMessage = validate.stream()
                    .anyMatch(violation -> "Count must be >= 10".equals(violation.getMessage()));

            assertFalse(hasExpectedMessage);
        }

    }
}
