package shpp.azaika.validation.constraints;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import shpp.azaika.validation.constraints.constraintvalidators.CheckEddrValidator;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({FIELD,METHOD,PARAMETER,ANNOTATION_TYPE})
@Retention(RUNTIME)
@Constraint(validatedBy = CheckEddrValidator.class)
public @interface CheckEddr {
    String message() default "{shpp.azaika.validation.constraints}";
    Class<?>[] groups() default { };
    Class<? extends Payload>[] payload() default {};
}
