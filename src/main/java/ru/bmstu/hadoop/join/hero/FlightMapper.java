package ru.bmstu.hadoop.join.hero;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Optional;

public class FlightMapper extends Mapper<LongWritable, Text, FlightWritable, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Optional<FlightWritable> opt = FlightWritable.read(value.toString()).filter(v -> Float.compare(v.getDelay(), 0.0f) > 0);
        if (opt.isPresent()) {
            context.write(opt.get(), new LongWritable(1));
        }
    }
}