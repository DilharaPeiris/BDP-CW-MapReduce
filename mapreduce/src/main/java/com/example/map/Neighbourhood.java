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
import java.util.*;

public class Neighbourhood {

    public static class NeighbourhoodMapper extends
            Mapper<Object, Text, NullWritable, NullWritable> {

        public static final String REGION_COUNTER_GROUP = "Region";



        static List<String> regions = Arrays.asList( new String[]{"Central Region","North Region","East Region","West Region","North-East Region"});




        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {


            Map<String, String> parsed = Util.transformCsvToMapForNaiberhood(value
                    .toString());

            // Get the value for the Location attribute
            String location = parsed.get("region");
            System.out.println("test1 :" + location);


            // Look for a state abbreviation code if the location is not null or
            // empty
            if (location != null && !location.isEmpty()) {
//                context.getCounter(STATE_COUNTER_GROUP, location)
//                        .increment(1);
                boolean unknown = true;
                // Make location uppercase and split on white space
                String tokens = location.toUpperCase();

                if(regions.contains(location)){
                    context.getCounter(REGION_COUNTER_GROUP, location)
                            .increment(1);
                }




            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: Neighbourhood <users> <out>");
            System.exit(2);
        }

        Path input = new Path(otherArgs[0]);
        Path outputDir = new Path(otherArgs[1]);

        Job job = Job.getInstance(conf, "Count Num Users By Region");
        job.setJarByClass(Neighbourhood.class);

        job.setMapperClass(Neighbourhood.NeighbourhoodMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        int code = job.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            for (Counter counter : job.getCounters().getGroup(
                    Neighbourhood.NeighbourhoodMapper.REGION_COUNTER_GROUP)) {
                System.out.println(counter.getDisplayName() + "\t"
                        + counter.getValue());
            }
        }

        // Clean up empty output directory
        FileSystem.get(conf).delete(outputDir, true);

        System.exit(code);
    }
}
