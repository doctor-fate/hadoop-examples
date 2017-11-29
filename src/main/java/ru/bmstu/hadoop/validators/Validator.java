package ru.bmstu.hadoop.validators;

import org.apache.commons.validator.routines.FloatValidator;
import org.apache.commons.validator.routines.IntegerValidator;

import java.util.Optional;

public class Validator {
    private static final IntegerValidator INTEGER_VALIDATOR = IntegerValidator.getInstance();
    private static final FloatValidator FLOAT_VALIDATOR = FloatValidator.getInstance();

    public static Optional<Integer> validateInteger(String value) {
        return Optional.ofNullable(INTEGER_VALIDATOR.validate(value));
    }

    public static Optional<Float> validateFloat(String value) {
        return Optional.ofNullable(FLOAT_VALIDATOR.validate(value));
    }
}
