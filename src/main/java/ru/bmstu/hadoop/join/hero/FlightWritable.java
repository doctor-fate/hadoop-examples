package ru.bmstu.hadoop.join.hero;

import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;
import ru.bmstu.hadoop.validators.Validator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class FlightWritable implements WritableComparable<FlightWritable> {
    private static final int DAY_OF_WEEK_CSV_IDX = 4;
    private static final int WHEELS_ON_CSV_IDX = 15;
    private static final int DELAY_CSV_IDX = 18;
    private int day;
    private int hour;
    private float delay;

    @SuppressWarnings("unused")
    public FlightWritable() { }

    private FlightWritable(int day, int hour, float delay) {
        this.day = day;
        this.hour = hour;
        this.delay = delay;
    }

    static Optional<FlightWritable> read(@NotNull String input) {
        String[] splitted = input.replaceAll("\"", "").split(",");
        Optional<Integer> day = Validator.validateInteger(splitted[DAY_OF_WEEK_CSV_IDX]);
        Optional<Integer> time = Validator.validateInteger(splitted[WHEELS_ON_CSV_IDX]);
        Optional<Float> delay = Validator.validateFloat(splitted[DELAY_CSV_IDX]);
        return day.map(v -> new FlightWritable(v, (time.orElse(0) / 100) % 24, delay.orElse(0.0f)));
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(day);
        out.writeInt(hour);
        out.writeFloat(delay);
    }

    public void readFields(DataInput in) throws IOException {
        day = in.readInt();
        hour = in.readInt();
        delay = in.readFloat();
    }

    @Override
    public int compareTo(@NotNull FlightWritable o) {
        int result = Integer.compare(day, o.day);
        if (result == 0) {
            result = Integer.compare(hour, o.hour);
            if (result == 0) {
                return Float.compare(delay, o.delay);
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        FlightWritable that = (FlightWritable) o;
        return day == that.day && hour == that.hour && Float.compare(that.delay, delay) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(day, hour, delay);
    }

    @Override
    public String toString() {
        return String.format("%d\t%d", day, hour);
    }

    public int getDay() {
        return day;
    }

    public int getHour() {
        return hour;
    }

    public float getDelay() {
        return delay;
    }
}