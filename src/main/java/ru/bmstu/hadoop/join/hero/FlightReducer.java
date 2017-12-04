package ru.bmstu.hadoop.join.hero;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlightReducer extends Reducer<FlightWritable, LongWritable, FlightWritable, LongWritable> {
    @Override
    protected void reduce(FlightWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long result = 0;
        for (LongWritable v : values) {
            result += v.get();
        }
        context.write(key, new LongWritable(result));
    }
}