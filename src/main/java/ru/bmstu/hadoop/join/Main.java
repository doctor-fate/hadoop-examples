package ru.bmstu.hadoop.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main implements Tool {
    private Configuration configuration;

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(null, new Main(), args);
        System.exit(status);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration, "L2");
        job.setJarByClass(Main.class);

        job.setReducerClass(JoinReducer.class);
        job.setPartitionerClass(CompositeKeyAirportPartitioner.class);
        job.setGroupingComparatorClass(CompositeKeyAirportComparator.class);

        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(FlightAirportWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FlightJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirportJoinMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setNumReduceTasks(2);

        return job.waitForCompletion(true) ? 1 : 0;
    }

    public Configuration getConf() {
        return configuration;
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }
}