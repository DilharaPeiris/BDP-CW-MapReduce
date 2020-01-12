package com.example.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

public class Availability {

    public static class AvailabilityMapper extends
            Mapper<Object, Text, NullWritable, NullWritable> {

        public static final String STATE_COUNTER_GROUP = "State";

        private String[] statesArray = new String[]{"AL", "AK", "AZ", "AR",
                "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
                "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
                "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
                "OH", "OK", "OR", "PA", "RI", "SC", "SF", "TN", "TX", "UT",
                "VT", "VA", "WA", "WV", "WI", "WY"};

        private HashSet<String> states = new HashSet<String>(
                Arrays.asList(statesArray));

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {



            Map<String, String> parsed = Util.transformCsvToMapForAvailability(value
                    .toString());

            // Get the value for the Location attribute
            String location = parsed.get("availability");
            System.out.println("test1 :" + location);


            // Look for a state abbreviation code if the location is not null or
            // empty
            if (location != null && !location.isEmpty()) {
//                context.getCounter(STATE_COUNTER_GROUP, location)
//                        .increment(1);
                boolean unknown = true;
                // Make location uppercase and split on white space
                String tokens = location.toUpperCase();

                if (location.equalsIgnoreCase("365")) {
                    context.getCounter(STATE_COUNTER_GROUP, location)
                            .increment(1);
                }

                // For each token
//                for (String state : tokens) {
//                    // Check if it is a state
//                    if (states.contains(state)) {
//
//                        // If so, increment the state's counter by 1 and flag it
//                        // as not unknown
//                        context.getCounter(STATE_COUNTER_GROUP, state)
//                                .increment(1);
//                        unknown = false;
//                        break;
//                    }
//                }
//
//                // If the state is unknown, increment the counter
//                if (unknown) {
//                    context.getCounter(STATE_COUNTER_GROUP, "Unknown")
//                            .increment(1);
//                }
//            } else {
//                // If it is empty or null, increment the counter by 1
//                context.getCounter(STATE_COUNTER_GROUP, "NullOrEmpty")
//                        .increment(1);
//            }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: CountNumUsersByState <users> <out>");
            System.exit(2);
        }

        Path input = new Path(otherArgs[0]);
        Path outputDir = new Path(otherArgs[1]);

        Job job = Job.getInstance(conf, "Count Num Users By State");
        job.setJarByClass(Availability.class);

        job.setMapperClass(Availability.AvailabilityMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        int code = job.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            for (Counter counter : job.getCounters().getGroup(
                    Availability.AvailabilityMapper.STATE_COUNTER_GROUP)) {
                System.out.println("365 open count:" + "\t"
                        + counter.getValue());
            }
        }

        // Clean up empty output directory
        FileSystem.get(conf).delete(outputDir, true);

        System.exit(code);
    }
}
