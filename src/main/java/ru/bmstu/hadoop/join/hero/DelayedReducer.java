package ru.bmstu.hadoop.join.hero;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DelayedReducer extends Reducer<LongWritable, DelayedWritable, LongWritable, DelayedWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<DelayedWritable> values, Context context) throws IOException, InterruptedException {
        DelayedWritable w = null;
        for (DelayedWritable v : values) {
            if (w == null || v.compareTo(w) > 0) {
                w = new DelayedWritable(v.getHour(), v.getDelayed());
            }
        }
        if (w != null) {
            context.write(key, w);
        }
    }
}