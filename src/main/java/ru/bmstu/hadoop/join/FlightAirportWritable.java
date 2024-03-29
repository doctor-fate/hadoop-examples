package ru.bmstu.hadoop.join;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("unchecked")
public class FlightAirportWritable extends GenericWritable {
    private static Class<? extends Writable>[] CLASSES;

    static {
        CLASSES = (Class<? extends Writable>[]) new Class[]{
                FlightWritable.class,
                AirportWritable.class,
        };
    }

    @SuppressWarnings("unused")
    FlightAirportWritable() { }

    FlightAirportWritable(Writable instance) {
        set(instance);
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }
}
