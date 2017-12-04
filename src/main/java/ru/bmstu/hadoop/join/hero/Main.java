package ru.bmstu.hadoop.join.hero;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
    private static final String INTERMEDIATE_OUTPUT_PATH = "intermediate-output";

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(null, new Main(), args);
        System.exit(status);
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        Job job1 = Job.getInstance(configuration);
        job1.setJarByClass(Main.class);

        job1.setMapperClass(FlightMapper.class);
        job1.setReducerClass(FlightReducer.class);
        job1.setPartitionerClass(FlightPartitioner.class);
        job1.setGroupingComparatorClass(FlightComparator.class);

        job1.setMapOutputKeyClass(FlightWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(FlightWritable.class);
        job1.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job1, new Path("FLIGHTS.csv"));
        FileOutputFormat.setOutputPath(job1, new Path(INTERMEDIATE_OUTPUT_PATH));

        job1.setNumReduceTasks(2);

        if (!job1.waitForCompletion(true)) {
            return 1;
        }

        Job job2 = Job.getInstance(configuration);
        job2.setJarByClass(Main.class);

        job2.setMapperClass(DelayedMapper.class);
        job2.setReducerClass(DelayedReducer.class);

        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(DelayedWritable.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(DelayedWritable.class);

        FileInputFormat.addInputPath(job2, new Path(INTERMEDIATE_OUTPUT_PATH));
        FileOutputFormat.setOutputPath(job2, new Path("output"));

        job2.setNumReduceTasks(2);

        return job2.waitForCompletion(true) ? 0 : 1;
    }
}
