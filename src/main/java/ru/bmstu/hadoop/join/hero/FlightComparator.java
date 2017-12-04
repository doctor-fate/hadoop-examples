package ru.bmstu.hadoop.join.hero;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlightComparator extends WritableComparator {
    public FlightComparator() {
        super(FlightWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlightWritable x = (FlightWritable) a;
        FlightWritable y = (FlightWritable) b;

        int result = Integer.compare(x.getDay(), y.getDay());
        if (result == 0) {
            return Integer.compare(x.getHour(), y.getHour());
        }
        return result;
    }
}