package ru.bmstu.hadoop.join.hero;

import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class DelayedWritable implements WritableComparable<DelayedWritable> {
    private int hour;
    private int delayed;

    @SuppressWarnings("unused")
    public DelayedWritable() { }

    DelayedWritable(int hour, int delayed) {
        this.hour = hour;
        this.delayed = delayed;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(hour);
        out.writeInt(delayed);
    }

    public void readFields(DataInput in) throws IOException {
        hour = in.readInt();
        delayed = in.readInt();
    }

    @Override
    public int compareTo(@NotNull DelayedWritable o) {
        return Integer.compare(delayed, o.delayed);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DelayedWritable that = (DelayedWritable) o;
        return hour == that.hour && delayed == that.delayed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(hour, delayed);
    }

    @Override
    public String toString() {
        return String.format("(%d,%d)", hour, delayed);
    }

    public int getDelayed() {
        return delayed;
    }

    public int getHour() {
        return hour;
    }
}