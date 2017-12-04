package ru.bmstu.hadoop.join.hero;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DelayedMapper extends Mapper<LongWritable, Text, LongWritable, DelayedWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitted = value.toString().split("\\s+");
        int day = Integer.parseInt(splitted[0]), hour = Integer.parseInt(splitted[1]), delayed = Integer.parseInt(splitted[2]);
        context.write(new LongWritable(day), new DelayedWritable(hour, delayed));
    }
}