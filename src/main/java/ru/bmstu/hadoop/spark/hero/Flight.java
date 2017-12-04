package ru.bmstu.hadoop.spark.hero;

import ru.bmstu.hadoop.validators.Validator;
import scala.Serializable;

import java.util.Optional;

class Flight implements Serializable {
    private static final int DESTINATION_CSV_IDX = 14;
    private static final int DELAY_CSV_IDX = 18;
    private int code;
    private float delay;

    private Flight(int code, float delay) {
        this.code = code;
        this.delay = delay;
    }

    static Flight read(String input) {
        String[] splitted = input.split(",");
        Optional<Integer> code = Validator.validateInteger(splitted[DESTINATION_CSV_IDX]);
        Optional<Float> delay = Validator.validateFloat(splitted[DELAY_CSV_IDX]);
        return new Flight(code.orElse(0), delay.orElse(0.0f));
    }

    public int getCode() {
        return code;
    }

    public float getDelay() {
        return delay;
    }
}
