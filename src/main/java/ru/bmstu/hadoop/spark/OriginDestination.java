package ru.bmstu.hadoop.spark;

import ru.bmstu.hadoop.validators.Validator;
import scala.Serializable;

import java.util.Optional;

public class OriginDestination implements Serializable {
    private static final int ORIGIN_CSV_IDX = 11;
    private static final int DESTINATION_CSV_IDX = 14;
    private int origin;
    private int destination;

    private OriginDestination(int origin, int destination) {
        this.origin = origin;
        this.destination = destination;
    }

    static OriginDestination read(String input) {
        String[] splitted = input.split(",");
        Optional<Integer> origin = Validator.validateInteger(splitted[ORIGIN_CSV_IDX]);
        Optional<Integer> destination = Validator.validateInteger(splitted[DESTINATION_CSV_IDX]);
        return new OriginDestination(origin.orElse(0), destination.orElse(0));
    }

    boolean isValid() {
        return origin != 0 && destination != 0;
    }

    int getOrigin() {
        return origin;
    }

    int getDestination() {
        return destination;
    }
}
