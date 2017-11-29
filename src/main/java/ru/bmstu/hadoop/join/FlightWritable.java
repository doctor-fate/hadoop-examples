package ru.bmstu.hadoop.join;

import org.apache.hadoop.io.Writable;
import ru.bmstu.hadoop.validators.Validator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class FlightWritable implements Writable {
    private static final int CODE_CSV_IDX = 14;
    private static final int DELAY_CSV_IDX = 18;
    private int code;
    private float delay;

    @SuppressWarnings("unused")
    public FlightWritable() { }

    private FlightWritable(int code, float delay) {
        this.code = code;
        this.delay = delay;
    }

    static Optional<FlightWritable> read(String input) {
        String[] splitted = input.replaceAll("\"", "").split(",");
        Optional<Integer> code = Validator.validateInteger(splitted[CODE_CSV_IDX]);
        Optional<Float> delay = Validator.validateFloat(splitted[DELAY_CSV_IDX]);
        return code.map(v -> new FlightWritable(v, delay.orElse(0.0f)));
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(code);
        out.writeFloat(delay);
    }

    public void readFields(DataInput in) throws IOException {
        code = in.readInt();
        delay = in.readFloat();
    }

    boolean isDelayed() {
        return Float.compare(delay, 0.0f) == 1;
    }

    float getDelay() {
        return delay;
    }

    int getCode() {
        return code;
    }
}