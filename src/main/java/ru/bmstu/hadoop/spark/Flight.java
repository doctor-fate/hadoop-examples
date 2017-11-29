package ru.bmstu.hadoop.spark;

import ru.bmstu.hadoop.validators.Validator;
import scala.Serializable;

import java.util.Optional;

class Flight implements Serializable {
    private static final int CANCELLED_CSV_IDX = 19;
    private static final int DELAY_CSV_IDX = 18;
    private boolean cancelled;
    private float delay;

    private Flight(boolean cancelled, float delay) {
        this.cancelled = cancelled;
        this.delay = delay;
    }

    static Flight read(String input) {
        String[] splitted = input.split(",");
        boolean cancelled = splitted[CANCELLED_CSV_IDX].equals("1.00");
        Optional<Float> delay = Validator.validateFloat(splitted[DELAY_CSV_IDX]);
        return new Flight(cancelled, delay.orElse(0.0f));
    }

    boolean isCancelledOrDelayed() {
        return cancelled || Float.compare(delay, 0.0f) > 0;
    }

    float getDelay() {
        return delay;
    }
}
