package ru.bmstu.hadoop.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Optional;

public class AirportJoinMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, FlightAirportWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Optional<AirportWritable> opt = AirportWritable.read(value.toString());
        if (opt.isPresent()) {
            AirportWritable w = opt.get();
            context.write(new CompositeKeyWritable(w.getCode(), 0), new FlightAirportWritable(w));
        }
    }
}