package ru.bmstu.hadoop.spark.hero;

import ru.bmstu.hadoop.validators.Validator;
import scala.Serializable;

import java.util.Optional;

class Airport implements Serializable {
    private static final int ID_CSV_IDX = 0;
    private static final int NAME_CSV_IDX = 1;
    private int code;
    private String name;

    private Airport(int code, String name) {
        this.code = code;
        this.name = name;
    }

    static Airport read(String input) {
        String[] splitted = input.replaceAll("\"", "").split(",", 2);
        String name = splitted[NAME_CSV_IDX];
        Optional<Integer> id = Validator.validateInteger(splitted[ID_CSV_IDX]);
        return new Airport(id.orElse(0), name);
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
