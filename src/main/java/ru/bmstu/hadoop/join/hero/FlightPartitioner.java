package ru.bmstu.hadoop.join.hero;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlightPartitioner extends Partitioner<FlightWritable, LongWritable> {
    @Override
    public int getPartition(FlightWritable key, LongWritable w, int i) {
        return key.getDay() % i;
    }
}