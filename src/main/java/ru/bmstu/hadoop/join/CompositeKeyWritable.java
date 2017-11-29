package ru.bmstu.hadoop.join;

import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {
    private int airport;
    private int flag;

    @SuppressWarnings("unused")
    CompositeKeyWritable() { }

    CompositeKeyWritable(int airport, int flag) {
        this.airport = airport;
        this.flag = flag;
    }

    public int compareTo(@NotNull CompositeKeyWritable o) {
        int r = airport - o.airport;
        if (r == 0) {
            return flag - o.flag;
        }

        return r;
    }

    @Override
    public int hashCode() {
        int result = airport;
        result = 31 * result + flag;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompositeKeyWritable w = (CompositeKeyWritable) o;
        return airport == w.airport && flag == w.flag;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(airport);
        out.writeInt(flag);
    }

    public void readFields(DataInput in) throws IOException {
        airport = in.readInt();
        flag = in.readInt();
    }

    int getAirport() {
        return airport;
    }
}